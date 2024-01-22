"""
Date: 2022-02-18
Author: Kezhi Zuo
Objective: We have hundreds of zurich tests, and currently we do not have a way to export all the test names at once. This script can print the names of all the tests for zurich models. This script has three functions: the first one is a recursion function that go through different level of folders and find the files that match the conditions, here we get the paths of all the fra schema.yml files in zurich; the second one is to get the paths of all the schema.yml files that contain tests config; the third one is to load those schema.yml files as dictionaries, parse them, and print out the model names, column names and test names as how dbt format them. These three functions can be used separately or edited for different purposes, feel free to borrow as you need.
"""

# import libraries
import os
from os import listdir
from os.path import join, isfile, isdir, join
from os.path import join, isfile, isdir, join
import yaml


# get all the schema.yml files paths
def get_all_config_files(path="/app/models/fra"):
    files = [
        join(path, f)
        for f in listdir(path)
        if isfile(join(path, f))
        and f
        == "schema.yml"  # if need other yml files, can change it as f.endswith(".yml")
    ]
    dirs = [d for d in listdir(path) if isdir(join(path, d))]
    for d in dirs:
        files_in_d = get_all_config_files(join(path, d))
        if files_in_d:
            files.extend(files_in_d)
    return files


# get all the paths of schema.yml files that contains test
def get_tests_files(files):
    test_file = []
    for f_name in files:
        with open(f_name, "r") as f:
            content = f.read()
            if "tests:" in content:
                if f_name not in test_file:
                    test_file.append(f_name)
    return test_file


# load the above yml files as a dictionary and parse the content, then print them as the format of the test name
def get_tests(test_file):
    for yml in test_file:
        stream = open(yml, "r+")
        try:
            parsed_yaml = yaml.full_load(stream)
            for models in parsed_yaml["models"]:
                for k, v in models.items():
                    for i in v:
                        if "tests" in i:
                            test_model = models[
                                "name"
                            ]  # if you want to print only the model names that contain tests, you can replace the following 5 lines with 1 line - print(test_model)
                            test_column = i["name"]
                            test_name = i["tests"]
                            for t in test_name:
                                tests = "{}_{}_{}".format(t, test_model, test_column)
                                print(tests)
        except yaml.YAMLError as exc:
            print(exc)


if __name__ == "__main__":
    get_tests(get_tests_files(get_all_config_files()))
