# Data Analytics Toolkit
Data Analytics toolket for making a comprehensive data analytics product

### 1) CONNECT TO DATA
#### sagemakerConnect.py
`Using AWS sagemaker to conduct data analysis and machine learning.`<br /> 
`This file include functions of connecting data in S3 Bucket from Sagemaker.` <br /> 
`So far, AWS provides so many tool sets for big data analytics and machine learning. ` <br /> 
`AWS Sagemaker is a powerful and easy-to-use service for machine learning. ` <br /> 
`Strong recommendation to check out AWS Sagemaker if you are struggling which platform to use for machine learning! `<br /> 

### 2) CONSUME DATA & MODELING/MACHINE LEARNING
#### preprocessing.py
`Funcitons frequently used for data imputations`


### 3) USER INTERFACE
#### visualization.py
`Funcitons frequently used for data visualization`


### 4) MAKE API
#### testAPIRequest.py
`Functions to test APIs using post methods. `
`Two types of request data format are included (i.e., XML, JSON requsts)`


### 5) AUTOMATION OF DEPLOYMENT
#### CREATE VIRTUAL ENVIRONMENT WHEN CREATING THE PROJECT
###### Create python virtual env
####`pip install virtualenv`
####`virtualenv carsapp`
####`myenv\Scripts\activate`

###### Save python depedencies
####`pip freeze - requirements.txt`

###### install python dependencies
####`pip install -r requirements.txt`

#### DEPLOYMENT TO AWS EC2 INSTANCE
`Chmod 400 cars-recommendation-test.pem `<br /> 
`Ssh -i cars-recommendation-test.pem ubuntu@<EC2_IP_ADDRESS> `<br /> 
`Sudo docker build -t <DOCKER_IMAGE> .`<br /> 
`Sudo docker run -p 5000:5000 -t <DOCKER_IMAGE>`<br /> 
`docker run -d -p 5000:5000 --name <DOCKER_IMAGE> <DOCKER_IMAGE>:latest`<br /> 


### 6) MAINTAINANCE 

