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

```title="requirements.txt"
--8<-- "requirements.txt"
```

Use command `pip3 install -r requirements.txt` to install all required Python requirements

### Optional requirements

Some Prime Backup features requires python libraries that does not listed in the requirements.txt,
because it might [take you some effort](https://github.com/oconnor663/blake3-py/issues/41) to install in some environments

If you want to have full features of Prime Backup, you can use the following command in advanced:

```bash
pip3 install blake3 lz4
```

These optional requirements are also stored in the [requirements.optional.txt](https://github.com/TISUnion/PrimeBackup/blob/master/requirements.optional.txt)

```title="requirements.optional.txt"
--8<-- "requirements.optional.txt"
```

### Install the plugin

Download Prime Backup from [GitHub Release](https://github.com/TISUnion/PrimeBackup/releases), 
and place it into the plugin folder of MCDR. Perform a [MCDR plugin reload](https://mcdreforged.readthedocs.io/en/latest/command.html#hot-reloads)

## Configure

Before using Prime Backup, you need to configure its config file correctly

Don't worry, for most of the config options, you can use the default values. 
But there are a few options that you might still want to take a look at

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

1. [Backup target](config.md#targets), i.e. the directories / files you want to create backup on. 
    You need to change the `"world"` in the `targets` array to your world directory name

    ```json
    // root config
    {
        // ...
        "backup": {
            "targets": [
                "world"
            ],
        }
        // ...
    }
    ```

    In addition, if you are using bukkit-like servers that split the world dimensions, you might want to use something like this:

     ```json
     "targets": [
         "world",
         "world_nether",
         "world_the_end"
     ]
     ```

2. The methods to calculate / store all backup data

    ```json
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
    
    - [`hash_method`](config.md#hash_method): The algorithm to hash the files. Available options: "xxh128", "sha256", "blake3"

        - [`"xxh128"`](https://github.com/Cyan4973/xxHash): A extremely fast, high-quality non-cryptographic hash algorithm. 
          Recommend to use, unless you want theoretic extreme safety on hackers
        - [`"sha256"`](https://en.wikipedia.org/wiki/SHA-2): A cryptographically secure and widely used hash algorithm
        - [`"blake3"`](https://en.wikipedia.org/wiki/SHA-2): A cryptographically secure and speedy hash algorithm. Much faster than sha256, but still slower than xxh128
          Recommend to use, don't forget to install the `blake3` Python requirement

    - [`compress_method`](config.md#compress_method): The way the backups get compressed. Common suggestions:

        - `"plain"`: No compression. Use this if you want the maximum operation speed
        - [`"zstd"`](https://github.com/facebook/zstd): Fast and good compression algorithm. Recommend to use you want to save some disk spaces
    
    !!! note
   
        If you want to use `blake3` as the hash method, you need to install the `blake3` python library manually.
        It's not included in the default requirement list, because in some environments it might require rust runtime to build and install
   
        ```bash
        pip3 install blake3
        ```
    
    !!! note

        It is recommended that you set these two options wisely from the start

        Although you can still use the `!!pb database migrate_xxx` command in MCDR to migrate the compression method and hash method of existing backups,
        completing the migration might require a certain amount of time and disk space

3. Enable the plugin. Set the `enabled` option in the root object to `true`. It should be at the top of the whole config file

    ```json
    // root config
    {
        "enabled": true
        // ...
    }
    ```

4. Reload the plugin with MCDR command

    ```text
    !!MCDR plugin reload prime_backup
    ```

Now Prime Backup should start working

## Use

Enter `!!pb` in the MCDR console, or in game, you should see the welcome page as shown below

![welcome](img/pb_welcome.png)
