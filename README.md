# cubejs-metabase-driver
Cube.js driver for Metabase.
## Note
The driver is under development so expect some bugs and missing features. If you find one of them please create an issue.
# Installation
## Get the driver
### Download
Download from the [releases](https://github.com/lili-data/metabase-cubejs-driver/releases).
### Build with Docker
1. Go to the `build` directory
2. Build the container first: `./image.sh`
3. Build the driver in the newly created container: `./driver.sh`  
   (this will create a `target` directory in the project root)

### Build without Docker
[Use this guide.](https://github.com/tlrobinson/metabase-http-driver/blob/master/README.md#building-the-driver)

## Copy to your metabase plugins
`cp your-driver.jar /path/to/metabase/plugins/`  
Note: you have to restart Metabase to load new plugins

# Development
## Roadmap
v1.0.0
- [x] FIX time dimensions (atm always return with nil)
- [ ] Fill the result with dummy columns, because if the result has fewer cols than the MBQL has error returned
- [x] Inspect and implement mbql->cubejs for "simple" dimensions
- [ ] Implement mbql->cubejs for order-by
- [ ] Local DB for testing
  - [ ] Fill with data
  - [ ] Add it to compose
  - [ ] Create schemas
  - [ ] Use it in Cube.js
- [ ] Write tests

## Testing the driver
1. Go to the `test` directory
2. Build the Cube.js image: `./build.sh`
3. Set the environment variables in the `cubejs.env` file
4. Copy the already built driver to the `driver` dir: `./copy.sh`
5. Start with Docker Compose: `docker-compose up`

# Contributing
- Any type of contributions are welcomed
- If you find a bug, missing feature or a simple typo just create an issue
- If you can fix/implement it create a pull request

# License
[GNU Affero General Public License v3.0 (AGPL)](https://github.com/lili-data/metabase-cubejs-driver/blob/master/LICENSE)