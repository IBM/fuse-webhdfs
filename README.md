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
pip3 install fuse-webhdfs
```

and this install fuse-webhdfs with all its dependencies

## IBM CEDP users

Please run the following script from the contrib directory:

```
contrib/ibm-cedp-webhdfs-setup
```

and provide your w3 SSO credentials

# General Usage

In one terminal type:

```
mkdir -p ~/fuse-webhdfs
python3 mount-webhdfs.py ~/fuse-webhdfs
```
You now have to type in your HDFS endpoint parameters and HDFS (Knox) username and password.

All parameters (including your password) will be saved in plain text in `$HOME/.config/webhdfs.ini`.
If this is an issue for you, please create a pull request and I will be happy to consider merging it.


After mounting, in other terminal(s) you will be able to list files, read them, etc.
For example:

```
ls -l ~/fuse-webhdfs/tmp
echo "this is a test" > ~/fuse-webhdfs/tmp/test
cat ~/fuse-webhdfs/tmp/test
```

