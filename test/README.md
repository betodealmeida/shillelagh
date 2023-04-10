# Shillelagh connection test

To execute this text:

1. Clone the [NGLS Shillelagh repository](https://github.com/GCL-UCC-NG911/shillelagh)
1. Create a python virtual environment.
1. Install the `requirements\test.txt` in the virtual environment.
1. Run the test script.

## Clone the [NGLS Shillelagh repository](https://github.com/GCL-UCC-NG911/shillelagh)

```bash
cd <path/to/git sources>
git clone https://<authentication@github.com/GCL-UCC-NG911/shillelagh
```

## Create a python virtual environment

**Important**

- On Windows I was only successful in executing this step using python 3.8. Something in the python build tools for 3.11 is preventing a clean build of the `greenlet` package.
- You can also not reuse the NGLS project's virtual environment (your workspace's venv) as there are quite a few incompatible packages that slow down the install to a crawl.

```bash
cd <path/to/git sources>/shillelagh
python -m venv .venv
```

## Install the `requirements\test.txt` in the virtual environment

**MAC**

```bash
cd <path/to/git sources>/shillelagh
.venv/bin/activate
pip install -r requirements/test.txt
```

**Windows**

```bash
cd <path/to/git sources>/shillelagh
.venv\Scripts\activate
pip install -r requirements\test.txt
```

## Run the test script

If you load the shillelagh project Folder in VS Code, then you will be able to **step through** the provided python test script under the test directory. A `launch.json` VS Code file has been added to suport this.

The following environment variables are used by the test program:

| Environment variable | Required | Default | Description |
| - | - | - | - |
| `CA_CERT_FILE` | Yes | | The location of the `<path/to/git sources>/ngls/tools/ca/certs/ca.crt` needs to be available to the script for the connection to the NGLS backend. |
| `NGLS_API_KEY` | Yes | | The NGLS API key is required to connect with the NGLS backend. |
| `NGLS_SERVER` | No | `ngls.mshome.net` (*) | The FQDN of the running NGLS backend. |

(*) MAC users: Make sure `ngls.mshome.net` is in `/etc/hosts` with the NGLS Ubuntu VM IP address. If not, add it to your `/etc/hosts` file.

### How to set the environment variables

There are 2 ways to set the required environment variables:

1. Adding them as environment variables on your local PC/MACbook.
1. Adding them to the `launch.json` file. This way is not really preferred as that file is checked in and your changes are really local to your environment.

#### Windows

```bash
# From your Powershell prompt (preferably with admin rights for Chess PCs):
PS> SystemPropertiesAdvanced.exe
# Click the 'Environment variables...' button
# Add NGLS_API_KEY and CA_CERT_FILE under User Variables
# Reload Visual Studio Code for the environment variables to be seen.
```

#### MAC books

Add the environment variables to your `/etc/environment` file.

## Troubleshooting

### Windows users

**Chess PCs** have the following issue:

```code
  Attempting uninstall: pip
    Found existing installation: pip 20.2.3
    Uninstalling pip-20.2.3:
      Successfully uninstalled pip-20.2.3
ERROR: Could not install packages due to an EnvironmentError: [WinError 5] Access is denied: 'C:\\Users\\A475695\\AppData\\Local\\Temp\\pip-uninstall-darzd06s\\pip.exe'
Consider using the `--user` option or check the permissions.
```

Just re-run the `pip install -r requirements\test.txt` a second time. The error will disappear.
