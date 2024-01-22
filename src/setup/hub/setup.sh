#!/bin/bash

if [ -e "$HOME/n26creds.yaml" ]; then 
	echo "Your n26creds.yaml file is already in the right place!"; 
else 
	cp n26creds_shell.yaml ~/n26creds.yaml
; fi


