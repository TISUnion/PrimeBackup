---
title: 'Quick Start'
---

## Install

### Prepare MCDR

Prime Backup is a [MCDReforged](https://github.com/Fallen-Breath/MCDReforged) plugin,
and it requires `mcdreforged>=2.12` to work 

To install / update MCDReforged, you can run:

```bash
pip3 install mcdreforged>=2.12 -U
```

See [MCDR document](https://mcdreforged.readthedocs.io/en/latest/quick_start.html) for more information

### Install Python requirements

Prime Backup requires a few python libraries to run, they are all listed in the
[requirements.txt](https://github.com/TISUnion/PrimeBackup/blob/master/requirements.txt) at the [GitHub repository](https://github.com/TISUnion/PrimeBackup) root

```bash title="requirements.txt"
--8<-- "requirements.txt"
```

Use command `pip3 install -r requirements.txt` to install all required Python requirements

### Install the plugin

Download Prime Backup from [GitHub Release](https://github.com/TISUnion/PrimeBackup/releases), 
and place it into the plugin folder of MCDR. Perform a [MCDR plugin reload](https://mcdreforged.readthedocs.io/en/latest/command.html#hot-reloads)

## Configure

Before using Prime Backup, you need to configure its config file correctly

Don't worry, for most of the config options, you can use the default values. 
But there's a few options that you still need to configure carefully

### Location

When Prime Backup is firstly loaded by MCDR, it will automatically generate the config file,
with the location at `config/prime_backup/config.json`

```bash
mcdr_root/
└── config/
    └── prime_backup/
        └── config.json       <-------------
```

It's a json file, so you need to follow the json syntax to edit it

### Necessary configs

Here are a few important things in the config file:

1. The backup format you want to use

    ```js
    // root config
    {
        // ...
        "backup": {
            "hash_method": "xxh128",
            "compress_method": "zstd",
        }
        // ...
    }
    ```
   
    - [`hash_method`](config.md#hash_method): The algorithm to hash the files. Available options: "xxh128", "sha256"

        - [`"xxh128"`](https://github.com/Cyan4973/xxHash): A extremely fast, high-quality 128bit non-cryptographic hash algorithm. 
          Recommend to use, unless you want theoretic extreme safety on hackers
        - [`"sha256"`](https://en.wikipedia.org/wiki/SHA-2): A cryptographically secure and widely used 256bit hash algorithm.
          It's slower than xxh128, but the speed could be acceptable with modern hardware

    - [`compress_method`](config.md#compress_method): The way the backups get compressed. Common suggestions:

        - `"plain"`: No compression. Use this if you want the maximum operation speed
        - [`"zstd"`](https://github.com/facebook/zstd): Fast and good compression algorithm. Recommend to use you want to save some disk spaces
    
    !!! danger
    
        You **CANNOT** change the `hash_method` after the plugin is enabled. Make your choice wisely
    
    !!! warning

        Changing `compress_method` will only affect new files in new backups

2. Enable the plugin. Set the `enabled` option in the root object to `true`. It should be at the top of the whole config file

    ```json
    // root config
    {
        "enabled": true
        // ...
    }
    ```

3. Reload the plugin with MCDR command

    ```text
    !!MCDR plugin reload prime_backup
    ```

Now Prime Backup should start working

## Use

Enter `!!pb` in the MCDR console, or in game, you should see the welcome page as shown below

![welcome](img/pb_welcome.png)
