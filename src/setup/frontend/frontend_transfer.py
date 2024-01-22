import json
import os
import shutil
from distutils.dir_util import copy_tree

import frontmatter
import jinja2
import numpy as np
import pandas as pd

notebooks_to_skip = [
    # just a tool, not an actual research report
    "/boston/src/research/tools/HTML_Creator.ipynb",
    # additional notebooks for reproducibility. Actual report is being added fine in another file
    "/boston/src/research/product/acquire/20201014_kyc_call_quality_impact_on_user_journey/DATA-8971-kyc-qa-1-query-merge.ipynb",
    "/boston/src/research/product/acquire/20201014_kyc_call_quality_impact_on_user_journey/DATA-8971-kyc-qa-2-prepare-data.ipynb",
]


# list to string conversion
def listToString(s):
    # initialize an empty string
    str1 = "\n - "
    # return string
    return str(str1.join(s))


# function to return missing values in columns
def missing_values():
    missing_df = research_df[research_df[column_check].isna().any(axis=1)]
    if missing_df.empty:
        missing_str = "Nothing missing"
        print(missing_str)
    else:
        print("\n\n The following articles have missing metadata as noted: \n")
        for row in range(0, len(missing_df)):
            missing = []
            missing.append(
                list(
                    missing_df[column_check]
                    .iloc[row][missing_df[column_check].iloc[row].isnull()]
                    .index
                )
            )
            missing_str = (
                "Article: "
                + str(missing_df["title"].iloc[row])
                + "\n"
                + "Location: "
                + str(missing_df["folder"].iloc[row])
                + "\nMissing Metadata: "
                + "\n - "
                + str(listToString(missing[0]))
                + "\n"
            )

            print(missing_str)


# create a directory tree for us to modify
dirname = os.path.dirname(__file__)
webdir = os.path.join(dirname, "static_copy")
if os.path.isdir(webdir):
    try:
        shutil.rmtree(webdir)
    except:
        print("Could not delete previous web folder")

# copy the directory over
copy_tree(os.path.join(dirname, "static"), os.path.join(dirname, "static_copy"))

# set up dataframe with metadata
research_df = pd.DataFrame(
    columns=[
        "region",
        "date",
        "author",
        "link",
        "title",
        "tags",
        "summary",
        "research_type",
        "folder",
        "html",
    ]
)

failed = []
# trawl the subdirectories
for folderName, subfolders, filenames in os.walk(os.getcwd() + "/research"):
    # for subfolder in subfolders:
    # move html file to the end of the list so the dataframe row gets created first
    # print(f"\n Searching in: {folderName}")
    for filename in filenames:
        if ".html" in filename:
            filenames.append(filenames.pop(filenames.index(filename)))
    for filename in filenames:
        if filename.endswith(".Rmd"):
            # print(f"    .Rmd found {filename}")
            try:
                with open(os.path.join(folderName, filename)) as f:
                    post = frontmatter.load(f)
                # get metadata
                research_df = research_df.append(
                    {
                        "region": post.get("region"),
                        "date": pd.to_datetime(post.get("date")).date(),
                        "author": post.get("author"),
                        "link": post.get("link"),
                        "title": post.get("title"),
                        "tags": post.get("tags"),
                        "summary": post.get("summary"),
                        "research_type": post.get("research_type"),
                        "folder": folderName,
                    },
                    ignore_index=True,
                )
            except:
                folder_file = os.path.join(folderName, filename)
                if folder_file in notebooks_to_skip:
                    print(
                        f"🚧 {folder_file} does not have metadata and is fine to be skipped since it's part of the whitelist, defined at the top of this document. Skipping..."
                    )
                else:
                    print(
                        f"❌ {folder_file} does not have metadata, please add metadata or remove from commit or add to list of notebook files we consciously skip and don't want to end up in the Boston Frontend."
                    )
                    failed.append(filename)

            # if link is empty, search folder for HTML file
        # TODO: insert iPython logic here, before searching for HTML files in this folder
        if filename.endswith(".ipynb"):
            # print(f"    .ipynb found {filename}")
            with open(os.path.join(folderName, filename)) as f:
                data = json.load(f)
            meta_dict = {}
            try:
                for i in data["cells"][0]["source"]:
                    if i.find(":") > 0:
                        # remove endline characters ("/n")
                        i = i.replace("\n", "")
                        pair = i.split(":", 1)
                        meta_dict.update({pair[0].lower(): pair[1]})

                research_df = research_df.append(
                    {
                        "region": meta_dict.get("region"),
                        "date": pd.to_datetime(meta_dict.get("date")).date(),
                        "author": meta_dict.get("author"),
                        "link": meta_dict.get("link"),
                        "title": meta_dict.get("title"),
                        "tags": meta_dict.get("tags"),
                        "summary": meta_dict.get("summary"),
                        "research_type": meta_dict.get("research_type"),
                        "folder": folderName,
                    },
                    ignore_index=True,
                )
            except:
                folder_file = os.path.join(folderName, filename)
                if folder_file in notebooks_to_skip:
                    print(
                        f"🚧 {folder_file} does not have metadata and is fine to be skipped since it's part of the whitelist, defined at the top of this document. Skipping..."
                    )
                else:
                    print(
                        f"❌ {folder_file} does not have metadata, please add metadata or remove from commit or add to list of notebook files we consciously skip and don't want to end up in the Boston Frontend."
                    )
                    failed.append(filename)

        if filename.endswith(".html"):
            link_info = research_df.loc[research_df["folder"] == folderName]["link"]
            research_df.loc[research_df["folder"] == folderName, "html"] = str(
                "/reports/" + filename
            )
            # add file to static_copy/reports for S3
            shutil.copy(
                os.path.join(folderName, filename),
                os.path.join(
                    os.getcwd(), "setup/frontend/static_copy/reports", filename
                ),
            )

# This is breaking the build if we have notebooks in the PR that aren't properly being processed by Boston, preventing people
# from merging PRs where they think they add research to Boston, which then doesn't work.
assert (
    len(failed) == 0
), f"There are {len(failed)} notebook files without metadata. See above ❌ for list of files. Please fix these."

# Define file icons
research_df["html_icon"] = np.where(
    (research_df["html"].isnull()), '"//:0"', "images/file-html-32.png"
)  # sets source to null if html is empty, otherwise passes html image source
research_df["link_icon"] = np.where(
    (research_df["link"].isnull()), '"//:0"', "images/file-gdrive.png"
)  # sets link to null if html is empty, otherwise passes link image source

# Define github link
research_df["github"] = pd.Series(research_df["folder"]).str.replace(
    "/boston/", "https://github.com/n26/boston/tree/master/", regex=False
)

# Define research segment from directory name
research_df["segment"] = np.where(
    research_df["folder"].str.split("/").str[4]
    == "growth",  # create exception for growth since it doesn't have a 2nd level
    research_df["folder"].str.split("/").str[4],
    research_df["folder"].str.split("/").str[4]
    + " > "
    + research_df["folder"].str.split("/").str[5],
)
research_df["segment"] = research_df["segment"].str.replace(
    "_", " ", regex=False
)  # replace underscore with spaces for aesthetics
research_df["segment"] = research_df[
    "segment"
].str.title()  # capitalize all words for aesthetics
research_df["segment"] = research_df["segment"].str.replace(
    "Us", "US", regex=False
)  # fix US segment name for aesthetics

# Fill Research Type blanks with Data Deep Dives
research_df["research_type"].fillna("Data Deep Dives", inplace=True)

# Add User Research Dataframe from CSV
ux_df = pd.read_csv(
    os.getcwd() + "/research/user_research/user_research_input.csv", dtype="category"
)
ux_df["link_icon"] = "images/file-gdrive.png"
ux_df["html_icon"] = ""
ux_df["github"] = ""
ux_df["folder"] = "src/research/user_research/user_research_input.csv"
ux_df["html"] = ""
ux_df["date"] = ux_df.apply(lambda x: pd.to_datetime(x["date"]).date(), axis=1)
ux_df = ux_df[
    [
        "region",
        "date",
        "author",
        "link",
        "title",
        "tags",
        "summary",
        "folder",
        "html",
        "html_icon",
        "link_icon",
        "github",
        "segment",
        "research_type",
    ]
]


# Add Feature Evaluation Dataframe from CSV
fe_df = pd.read_csv(
    os.getcwd() + "/research/feature_evaluation/feature_evaluation_input.csv",
    dtype="category",
)
fe_df["link_icon"] = "images/file-gdrive.png"
fe_df["html_icon"] = ""
fe_df["github"] = ""
fe_df["folder"] = "src/research/feature_evaluation/feature_evaluation_input.csv"
fe_df["html"] = ""
fe_df["date"] = fe_df.apply(lambda x: pd.to_datetime(x["date"]).date(), axis=1)
fe_df["research_type"] = "Feature Evaluation"
fe_df = fe_df[
    [
        "region",
        "date",
        "author",
        "link",
        "title",
        "tags",
        "summary",
        "folder",
        "html",
        "html_icon",
        "link_icon",
        "github",
        "segment",
        "research_type",
    ]
]

frames = [research_df, ux_df, fe_df]
research_df = pd.concat(frames)

# Add research clusters and remove empty spaces
clusters_df = pd.read_csv(
    os.getcwd() + "/research/research_clusters/research_clusters_input.csv"
)
research_df["title"] = research_df["title"].str.strip()
clusters_df["research_title"] = clusters_df["research_title"].str.strip()

# Group all research in the same cluster into a column and join it with clusters_df
grouped_clusters = clusters_df.groupby("cluster_name")
grouped_lists = grouped_clusters["research_title"].agg(
    lambda column: "   ".join(column)
)  # the spaces bars allow for us to trim later
grouped_lists = grouped_lists.reset_index(name="research_title_list")
clusters_df = pd.merge(
    clusters_df,
    grouped_lists,
    how="left",
    left_on="cluster_name",
    right_on="cluster_name",
)

# Remove research title in that row from the list and join with research_df
clusters_df["research_title_list"] = clusters_df.apply(
    lambda x: x["research_title_list"].replace(x["research_title"], ""), axis=1
)
clusters_df["research_title_list"] = clusters_df["research_title_list"].str.strip()
clusters_df["research_title_list"] = clusters_df["research_title_list"].str.replace(
    "  ", ","
)
clusters_df["research_title_list"] = clusters_df["research_title_list"].str.replace(
    ",,,", ", "
)
research_df = pd.merge(
    research_df, clusters_df, how="left", left_on="title", right_on="research_title"
)

# drop rows with no links or no html
research_df = research_df[(research_df["link"].notna()) | (research_df["html"].notna())]

# Deal with empty github links
research_df["github_icon"] = np.where(
    (research_df["github"] == ""), '"//:0"', "images/github-32.png"
)  # sets link to null if html is empty, otherwise passes link image source

# Trim regions and replace region names with flags
research_df["region"] = research_df["region"].str.strip()
research_df["region"] = research_df["region"].str.replace("EU", "🇪🇺")
research_df["region"] = research_df["region"].str.replace("US", "🇺🇸")
research_df["region"] = research_df["region"].str.replace("UK", "🇬🇧")
research_df["region"] = research_df["region"].str.replace("BR", "🇧🇷")
research_df["region"] = research_df["region"].str.replace("Global", "🌍")

# Fill Related Research NAs
research_df["research_title_list"].fillna("", inplace=True)

# Trim Author List
research_df["author"] = research_df["author"].str.strip()

# order by date
research_df = research_df.sort_values(by=["date", "author", "title"], ascending=False)

# drop repeated rows
research_df.drop_duplicates(subset="title", keep="first", inplace=True)

# Raise errors for missing main fields
column_check = ["region", "title", "author", "tags", "summary"]
if research_df[column_check].isnull().values.any():
    raise Exception(missing_values())

# Lists Clusters
clusters_list = clusters_df["cluster_name"].str.strip()  # remove blank spaces from tags
clusters_list = clusters_list.unique().tolist()

# Lists Authors
author_string = research_df.author.str.cat(
    sep=","
)  # concatenate all authors into a string
author_list = author_string.split(",")  # list all authors
author_list = pd.DataFrame(
    author_list, columns=["author"]
)  # convert to df (easier to trim)
author_list = author_list["author"].str.strip()  # remove blank spaces from authors
author_list = author_list.unique().tolist()  # create list to be passed to the template
author_list = sorted(author_list)

# Lists Regions
region_list = research_df["region"].unique().tolist()

# Lists Segments
segment_list = research_df.sort_values(by=["segment"])
segment_list = segment_list["segment"].unique().tolist()

# Lists Research Teams
research_type_list = [
    "Data Deep Dives",
    "User Research",
    "Growth Insights",
    "Feature Evaluation",
]  # Hardcoding this one to assure it stays stable

print("\n\n --- Final loaded research items ---")
print(research_df[["date", "title"]])
# convert to an iterable list
research_list = research_df.to_dict("records")
# get list length to pass to template
research_list_length = len(research_list)

# assumes the template is in the same directory as this script
templateLoader = jinja2.FileSystemLoader(searchpath="./")
templateEnv = jinja2.Environment(loader=templateLoader)
# load template by filename
TEMPLATE_FILE = "setup/frontend/index_template.html"
template = templateEnv.get_template(TEMPLATE_FILE)
# this is where to put args to the template renderer
outputText = template.render(
    research_list=research_list,
    author_list=author_list,
    region_list=region_list,
    clusters_list=clusters_list,
    research_list_length=research_list_length,
    segment_list=segment_list,
    research_type_list=research_type_list,
)

# save the index.html file
index = open(os.path.join(os.getcwd(), "setup/frontend/static_copy/index.html"), "w")
index.write(outputText)
index.close()
