# csloth-model-creator

Model creator repository - new development version

## Structure:

- `config`: all configs for project
- `data`: Data folder
- `importdb`: saving to db
- `notebooks`: Analysis jupyter notebooks 
- `server`: ecosystem@54.167.226.86

## Build

### Test

## Deploy

### Deploy to developer folder: 

Command: `fab deploy_dev:{username},{server_ip}`

Examples:

- fab deploy_dev:hungph

Project will be located at `/home/ecosystem/data/{username}/csloth-model-creator-v1/`

### Deploy to production folder: 

Command: `fab deploy_prod:{server_ip}`

Project will be located at  `/home/ecosystem/data/cs-server/csloth-model-creator-v1`

### Connect to mysql server
```shell script
mysql -u root -h 34.232.71.148 -p
mysql> show databases;
mysql> use cs_data; 
mysql> show tables; 
mysql> select * from cryptoquant;
```

# Environment

### Export environment

```shell script
conda env export > environment-dev-{os}.yml
```

### Clone environment

```shell script
conda env create -f environment-dev-{os}.yml
```