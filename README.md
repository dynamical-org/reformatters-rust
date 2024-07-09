# Reformatters

Reformatting code to create dynamical.org zarr archives from source weather and climate datasets.

Heavily in progress work.

### Development

1. [Install rust](https://www.rust-lang.org/tools/install)
1. [Install GDAL](https://gdal.org/download.html) (eg. `sudo apt install -y libgdal-dev` or `brew install gdal`)
   - If `cargo run` gives an error about a missing gdal library, make sure the library is on your LD_LIBRARY_PATH
1. Example command: `cargo run --bin gfs -- temperature_2m 2024-01-01T00:00:00Z 2024-02-01T00:00:00Z`
