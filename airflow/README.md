
set airflow connections

{
    Conn_id: 'aws_default',
    Conn_type: 'Amazon Web Services',
    Login: '<>', 
    Password: '<>', 
    Extra: '{ "region_name" : "us-east-1" }'
}

{
    Conn_id: 'emr_default',
    Conn_type: 'Elastic MapReduce',
    Extra: """
    {
        "Name": "indextracker",
        "ReleaseLabel": "emr-6.5.0",
        "LogUri": "s3://indextracker/emr/logs/",
        "Instances": {
            "InstanceGroups": [
                {
                    "Name": "Master nodes",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "MASTER",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 1
                },
                {
                    "Name": "Slave nodes",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "CORE",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 2
                }
            ],
            "TerminationProtected": false,
            "KeepJobFlowAliveWhenNoSteps": false
        },
        
        "ServiceRole": "EMR_DefaultRole",
        "JobFlowRole": "EMR_EC2_DefaultRole",

        "Applications": [
            {
                "Name": "Spark"
            }
        ]
    }
    """
}

{
    Conn_id: 'postgres_default',
    Conn_type: 'Postgres',
    Host: '<>',
    Schema: 'dev',
    Login: '<>',
    Password: '',
    Port: 5439, 
}
