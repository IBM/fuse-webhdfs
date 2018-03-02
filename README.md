# Description

Mount WebHDFS on your local Linux or Mac file system

After this, you can access the WebHDFS file system as if was a local directory - with regular Unix file operations.

# Installation

First dependency is fuse, you can install it on Ubuntu with:
```
sudo apt-get install fuse
```

or on RedHat with:
```
sudo yum install fuse
```

after that, you can simply run:

```
pip3 install fuse_webhdfs
```

and this should pull in all dependencies

## Preparing IBM internal certificates

If you're running on IBM network, you'll need IBM internal certificate chain. You can run the following commands to prepare this chain:

```
curl -L https://daymvs1.pok.ibm.com/ibmca/downloadCarootCert.do\?file\=carootcert.der | openssl x509 -inform DER -outform PEM > ibm-chain.crt
curl -L https://daymvs1.pok.ibm.com/ibmca/downloadCarootCert.do\?file\=caintermediatecert.der | openssl x509 -inform DER -outform PEM >> ibm-chain.crt
```

And in the following configuration step please provide 'ibm-chain.crt' as the HDFS web server certificate path.

# Usage

In one terminal type:

```
mkdir -p ~/fuse-webhdfs
python3 mount-webhdfs.py ~/fuse-webhdfs
```
You now have to type in your HDFS endpoint parameters and HDFS (Knox) username and password.
For IBM internal use, please provide your w3id username and password.

These parameters will be saved in plain text in `$HOME/.config/webhdfs.ini`.
If you have a problem with that, please create a pull request and I will be happy to consider merging it.


After mounting, in other terminal(s) you will be able to list files, read them, etc.
For example:

```
ls -l ~/fuse-webhdfs/tmp
echo "this is a test" > ~/fuse-webhdfs/tmp/test
cat ~/fuse-webhdfs/tmp/test
```

