#!/bin/bash

# who is this?
BRANCH="stef"

#retrieve gradplots output ?
RET_GRADPLOTS=0

#retrieve sqlite database ?
RET_DBASE=0

#push code to git
git commit -a -m "Running main.py on AWS Server at $(date +%H:%M--%h%m)"
git push

#ssh into server
ssh -i $HOME/SAkey.pem \
ubuntu@ec2-13-58-92-7.us-east-2.compute.amazonaws.com \
'source ~/.profile; bash -s' << \EOF

	#CD into code directory
	cd $HOME/analysis/Code

	#pull code from git 
	git checkout $BRANCH
	git pull 

	# run main.py
	cd $HOME/analysis/Code/LIGHTSTONE
	python main.py

EOF

if [ $RET_GRADPLOTS = 1 ]; then

	cd ../../Output/LIGHTSTONE

	RPATH="/home/ubuntu/analysis/Output/LIGHTSTONE/gradplots/"

	rsync --update -av -e "ssh -i $HOME/SAkey.pem" \
	ubuntu@ec2-18-216-234-87.us-east-2.compute.amazonaws.com:$RPATH gradplots/

fi

if [ $RET_DBASE = 1 ]; then

	RPATH="/home/ubuntu/analysis/Output/LIGHTSTONE/gradplots/"

	rsync --update -av -e "ssh -i $HOME/SAkey.pem" \
	ubuntu@ec2-18-216-234-87.us-east-2.compute.amazonaws.com:$RPATH gradplots/

fi