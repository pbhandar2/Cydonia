# Usage

To setup the system with 1TB disk file in /dev/sdb, 420GB NVM file in /dev/sdc and S3 buckets to upload output. 

``` sudo ./setup.sh /dev/sdb /dev/sdc 1000000 420000 $AWS_KEY $AWS_SECRET ```

# Examples 

The following are the setup commands for different cloudlab machines. The specs are taken from the 
cloudlab [website](https://docs.cloudlab.us/hardware.html) and updated on 16/06/2023.

TODO: update the run comman for each machine

- c220g1 
    - CPU: Two Intel E5-2630 v3 8-core CPUs at 2.40 GHz (Haswell w/ EM64T)
    - RAM: 128GB ECC Memory (8x 16 GB DDR4 1866 MHz dual rank RDIMMs)
    - Disk: Two 1.2 TB 10K RPM 6G SAS SFF HDDs
    - Disk: One Intel DC S3500 480 GB 6G SATA SSDs

``` sudo ./setup.sh /dev/sdb /dev/sdc 950000 450000 $AWS_KEY $AWS_SECRET ```

- c220g5 
    - CPU: Two Intel Xeon Silver 4114 10-core CPUs at 2.20 GHz
    - RAM: 192GB ECC DDR4-2666 Memory
    - Disk: One 1 TB 7200 RPM 6G SAS HDs
    - Disk: One Intel DC S3500 480 GB 6G SATA SSD

``` sudo ./setup.sh /dev/sdb /dev/sdc 9900000 470000 $AWS_KEY $AWS_SECRET ```

- r6525 
    - CPU: Two 32-core AMD 7543 at 2.8GHz
    - RAM: 256GB ECC Memory (16x 16 GB 3200MHz DDR4)
    - Disk: One 480GB SATA SSD
    - Disk: One 1.6TB NVMe SSD (PCIe v4.0)

``` sudo ./setup.sh /dev/sdb /dev/sdc 4700000 1500000 $AWS_KEY $AWS_SECRET ```


