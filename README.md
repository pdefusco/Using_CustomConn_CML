# Using Custom Connections in CML

Cloudera Machine Learning (CML) is Cloudera’s new cloud-native machine learning service, built for CDP. Cloudera Machine Learning unifies self-service data science and data engineering in a single, portable service as part of an enterprise data cloud for multi-function analytics on data anywhere.

Data Scientists and Engineers use CML to securely analyze large amounts of data via interactive notebooks. Since 2022 CML enhances this capability with the [Data Connections](https://community.cloudera.com/t5/Community-Articles/New-Feature-in-Cloudera-Machine-Learning-Data-Connections/ta-p/336775) feature by providing boiler template code to access and push down SQL queries to the CDW service. This allows the user to run large scale queries directly in the Data Warehouse while accessing and visualizing results from a simple notebook with a small consumption profile.

In 2023 CML added new capabilities to this feature with CML [*Custom* Data Connections](https://docs.cloudera.com/machine-learning/cloud/mlde/topics/ml-custom-data-conn-create.html). CCD allows the CML user to create their own boiler template code so they can connect to 3rd party tools such as Snowflake, Postgres, and others.

This repository is corollary to this CML Blog Article and provides the code to reproduce in your CML Workspace (version 2.0.38 or above required).
