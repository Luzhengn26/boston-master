# Boston: N26 Research Repository

![Boston](https://upload.wikimedia.org/wikipedia/commons/c/c6/Boston_skyline_at_earlymorning.jpg)

<p align="center">
    <a href="https://app.slack.com/client/T04SHSQPC/G9L7F8WJE">
    <img src="https://cdn.worldvectorlogo.com/logos/slack-new-logo.svg" width="32" height="16">
    </a>
    -|-
    <a href="https://jenkins-default-fra.tech26.de/blue/organizations/jenkins/data%2Fboston/activity" target="_blank">
    <img src="https://cdn.worldvectorlogo.com/logos/jenkins.svg" width="72" height="19">
    </a>
    -|-
    <a href="https://kibana-live-fra.tech26.de/goto/ba1cc312efa3473f13c38869689389bd" target="_blank">
    <img src="https://cdn.worldvectorlogo.com/logos/elastic-kibana.svg" width="32" height="20">
    </a>
</p>

## What is N26 Research?

N26 Research is our internal research hub. Our aim is to accelerate the pace of innovation within our organization by enabling knowledge sharing and efficient collaboration through effortless discovery of insights. All research displayed here resides on our Github repository Boston (only available for those with access to the N26 Github).

## What is Boston?

Boston has several main functions:

1. A centralized storage place for all research in the organization, whether that be written in R, Python, SQL or is simply a Google Slides presentation.
2. Present all research in the organization on the [Research Hub frontend](https://research.tech26.de/) which is accessible to all N26’ers with TZ2 VPN.

---

# How to create new research with Boston

<details>

<summary>Python</summary>

<p> In order to get started with Boston, first follow the following steps:

1. Set the region applicable to your work in `compose/docker-notebook.yaml` under N26_REGION.
    - Expected values are N26 AWS regions, which at time of writing are either `fra` (Frankfurt/EU) or `ore` (Oregon/US).
    - Working with EU data:
    ```
    environment:
      - N26_REGION=fra
      - REDSHIFT_USER=${REDSHIFT_USER}
      - REDSHIFT_PASSWORD=${REDSHIFT_PASSWORD}
    ```
    - Working with US data:
    ```
    environment:
      - N26_REGION=ore
      - REDSHIFT_USER=${REDSHIFT_US_USER}
      - REDSHIFT_PASSWORD=${REDSHIFT_US_PASSWORD}
    ```
2. Start a local Boston notebook docker container.
    - Run in Terminal: `make notebook`
    - Wait for the build and startup to finish. You should see something like this:
    ```
    notebook_X  |     To access the server, open this file in a browser:
    notebook_X  |         abcdefg/hijklmnop/qrstuvwxyz
    notebook_X  |     Or copy and paste one of these URLs:
    notebook_X  |         http://str1ngnum83rs:8888/lab?token=LONGTOKENNUMBER
    notebook_X  |         http://IPA.D.D.RESS:8888/lab?token=LONGTOKENNUMBER
    ```
    - Note: This will take some time when you run it for the first time.
    - Open one of the links provided in your terminal and you should be directed to a Jupyter Lab page by default.
3. Make sure to take a look at the `Get Started notebook` first! It contains a lot of tips and tricks for you to get started with your research.
4. Keep in mind that we only maintain basic functions (such as `df_from_sql` to query from our DWH) and library versions in this repository.
    - If you need to use these basic functions from the utils folder, make sure to run `cd /app/` in the beginning of your notebook (below the research metadata).
    - If you need a specific library for your research that does not come pre-installed, you can easily install it with the command `!pip install` (e.g. `!pip install altair`) in a Jupyter Notebook cell.
    - Make sure to declare the library versions used for your research, such that it can be replicated without issues in the future by others (or even yourself)!
5. To create HTML output, use the HTML creator saved in `src/research/tools/HTML_Creator.ipynb`. You need to paste your notebook name and the code will create an HTML file for you.

> :warning: **Python Code must be black linted**: Black linting is enforced for all python files including `*.ipynb` notebooks. To [install black](https://black.readthedocs.io/en/stable/getting_started.html#installation) in a way that also can handle notebooks run `python3 -m pip install --user --upgrade "black[jupyter]"` after installing you should be able to lint all files in Boston including notebooks by running `black .` from the main directory.

</p>

</details>

<details>

<summary>R</summary>

1. [Install R](https://mirror.las.iastate.edu/CRAN/) (note that some functions/libraries might not work on versions < 3.3)
2. [Install RStudio Desktop](https://rstudio.com/products/rstudio/download/)
3. Install and load the devtools package in RStudio

```R
install.packages("devtools")
library(devtools)
```

4. From RStudio, install the [DataN26 package](https://github.com/n26/DataN26). There are two ways to do this:
1. Run the following code: `devtools::install_github("n26/DataN26", auth_token = 'github auth token here` (this method potentially requires your token to have SSO enabled, and for the token to have Github package read/write access)
1. Clone the DataN26 repo and install from the local files: `devtools::install_local("~/local/path/to/repo")`
1. Run `source src/setup/hub/setup.sh` from the terminal to finalize setup
1. Enter your db credentials in the new ~/n26creds.yaml file that was just created
1. Load the DataN26 package in your R script with `library(n26)`. Run SQL queries from your script with the `queryDB()` function and save the output as a dataframe variable

</details>

---

# How to contribute existing research to Boston

<details>

<summary>Preparing your research for uploading to Boston</summary>

<p>
❗ Do NOT show any PII data in your notebook outputs.

To enable Boston to parse your research for the relevant metadata correctly, please follow the guidelines below. The more descriptive your metadata is, the better discoverable is your research on our Research Hub, so don’t sleep on this one!

You must **either supply an HTML output file** in your research folder **or specify a link (such as Google Slides URL)** as part of your metadata in your notebook header.

Keep your audience in mind as you prepare the research summary. Our Research Hub is open for anyone in the organization, and we invite colleagues from any business area to search and find insights here. Ideally, highly technical research at least provides a conclusion to non-technical readers.
Aim to include the SQL queries with which data is pulled such that the research is replicable.
</p>

## Metadata guidelines

- `date` should follow the `YYYY-MM-DD` format
- `region` should fall into these categories: EU, US, UK, BR and Global
- `link` can be to a Google Slides presentation or any other relevant URL. If no link is supplied, the first HTML file found in your research folder will be shown instead.
- `tags` could include: team, segment, country, metrics, method, model type, product name, platform, external partner, etc.
- `summary` should give an overview of what your research is about and what the main findings are. Other questions you may want to answer could include: what was the context of the research? How were the findings made actionable? What were next step recommendations? What were limitations?
- (optional) `research_type` is only needed if your research has contributions coming from several teams instead of just one. List of teams separated by comma. For example, if it was a joint effort between data and user researchers, your input would be `research_type: Data Deep Dives, User Research`

Do:

```
title: Amazing and Descriptive Title of Research
author: Author McAuthorson
date: 2020-05-21
region: EU
link: www.link_to_google_drive_file_if_available.com
tags: know your customer, kyc, acquire, signup, conversion, germany, linear regression
summary: This amazing research aimed to look into the future of research. The results were 100% positive.
research_type: Data Deep Dives, User Research, Growth Insights
```

Don't:

```
Title: You wil not beliebve what the Memberships Data Analyst has done! Click here to find out!!! (we aren’t a clickbait factory, and mind your typos)
author: Helder Silva and Claudia Dai (should be comma separated)
date: 21.05.2020 (wrong format)
region: Germany (not part of the regions you may specify)
link: www.download_research_here.zip (don’t link to suspicious URLs)
tags: know, your, customer, sign, up, aqcuire (never comma separate words that belong to a whole expression, and try to include as many descriptive tags as possible, and mind your typos)
summary: Insightful research with crazy results (not descriptive)
research_type: Product Analytics (not part of the teams you may specify)
```

## Metadata header examples

### Python

Add a single cell at the top of your notebook to contain the metadata with the following metadata.

![Metadata header example for Python notebooks](/src/img/header-example-python.png)

### R

Create an Rmd file and add the following metadata format at the top of the file.

![Metadata header example for Rmd](/src/img/header-example-rmd.png)

### Neither Python nor R (e.g. only SQL code and a presentation)

Include the above explained metadata in an Rmd or Ipynb file.

</details>

<details>

<summary>Uploading your prepared research to Boston</summary>

1. Create a new folder for your research in the appropriate directory under `/research/{segment}/...`

   - Each new research project should have its **own folder**
   - The folder should be **prefixed** with date in the format `YYYYMMDD_{RESEARCH_NAME}`
   - Give **descriptive** names to your research’s folder and files
   - Use **underscores** instead of spaces
   - Do NOT upload any large datasets
   - Any external **small** datasets should be included in a `/data` subfolder
   - **Do NOT show any PII data in your notebook outputs**
   - Aim to include the SQL queries with which data is pulled such that the research is replicable

   Do: `/research/20210101_awesome_research`
   Don’t: `/research/2021-01-01 not awesome research`

   Do: `/research/20210101_awesome_research`
   Don’t: `/research/not_awesome_research`

   Do: `/research/20210101_awesome_research`
   Don’t: `/research/not_awesome_research_20210101`

2. Create a new PR with your research folder and files and go through the checklist
3. When your checklist has been completed, have your PR thoroughly reviewed for both code quality and content by a colleague
4. Your research is automatically uploaded to the N26 Data Research Hub and will be live after your PR is merged into the Boston master.
</details>

<details>

<summary>Adding User Research articles</summary>

Once the User Research team sends <a href="https://docs.google.com/spreadsheets/d/1EM89wuWHTYQkModvw5yeC8ns5z5VAj7CiYEjw7eZRTw/edit#gid=0" target="_blank">this filled sheet</a> in your direction all you need to do is:
1. Download the sheet in a CSV and make sure it doesn’t break.
2. Add the new input into <a href="https://github.com/n26/boston/blob/master/src/research/user_research/user_research_input.csv" target="_blank">this CSV file</a> in Boston.
3. Test your changes in the frontend, making sure that the links work.
4. Push those changes into master.
</details>

<details>

<summary>Adding and Updating Research Clusters</summary>

You can use <a href="https://docs.google.com/spreadsheets/d/1ImaHUEVOXT9Nm-DWkFXHv66ci3GF_GFj_TWtcDJ43uo/edit#gid=0" target="_blank">this sheet</a> as a template to aggregate your research into a research cluster. As you can see, all you need is a cluster name that is common to all your articles in the same cluster. Once you have these you can:
1. Populate the sheet with the Cluster name and the research title. Do make sure that your title in this file matches exactly the title of your research.
2. Download the sheet in a CSV and make sure it doesn’t break.
3. Add the new input into this <a href="https://github.com/n26/boston/blob/master/src/research/research_clusters/research_clusters_input.csv" target="_blank">CSV file in Boston</a>.
4. Test your changes in the frontend, making sure that all your research is included in your new cluster by filtering for that cluster.
5. Push those changes into master.
</details>

---

# How to improve Boston

<details>

<summary>Testing the Web Frontend</summary>

1. From the root directory run `make up`. This will run the service and spin-up a local webserver at localhost on port 8000 (requires VPN)
2. Using any browser go to http://localhost:8000 to review output

To test the compilation script and frontend separately, you can do the following:

1. **Compilation script:** from the root directory run `./docker_build.sh` (requires Docker to be open)
2. **Local webserver:** from the root directory run `python3 src/setup/frontend/simplehttp.py` and navigate to http://localhost:8000/ in a browser

</details>
