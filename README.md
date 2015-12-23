# Learning Amazon Kinesis Development, Part One: Streams, Producers, and Consumers

The learning-module-1 branch provides the code for the first learning module, [Streams, Producers and Consumers][learning-kinesis-part-1], in the [Learning Kinesis Development][learning-kinesis] series.

[learning-kinesis]: http://docs.aws.amazon.com/kinesis/latest/dev/learning-kinesis.html
[learning-kinesis-part-1]: http://docs.aws.amazon.com/kinesis/latest/dev/learning-kinesis-module-one.html

## Eclipse setup
1. Git clone this project, and import cloned project into Eclipse. (resolve errors will show up but don't worry)
2. Download [“amazon-kinesis-client-1.1.0.jar”](http://mvnrepository.com/artifact/com.amazonaws/amazon-kinesis-client/1.1.0) and directly import it as external jar
3. [install "AWS toolkit for Eclipse"](http://docs.aws.amazon.com/AWSToolkitEclipse/latest/ug/tke_setup_install.html)
4. Project "Build Path" -> "Config Build Path" -> "Library" -> "Add Library" -> "AWS SDK for Java", then all build works
5. Create folder and file “~/.aws/credentials”, and content of "credentials" is like below:
```
[default]
aws_access_key_id=put_here_your_aws_access_key_id
aws_secret_access_key=put_here_your_aws_secret_access_key
```