# mimosa-hadoop

Maven project template for writing and submitting jobs to mimosa cluster

### Clone the project
git clone git@github.com:dapurv5/mimosa-hadoop.git

###Write your map-reduce job
Refer to NodeDegree.java for how to write a sample map-reduce job

### Change pom.xml
Change the pom.xml and specify the mainClass

### Build
mvn clean package

### Submit job
Copy your jar to any of the machines in the cluster and run the following command
```
hadoop jar mimosa-hadoop-1.0.jar <input-path-dir> <output-path-dir>
```

### View Progress of your jar after setting up socks proxy
https://docs.google.com/document/d/1zjlO7pLK1y42zOMUbyv0eTjTfeUyJZflP8_1WHS9rUs/edit?usp=sharing

