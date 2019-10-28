# cubejs-metabase-driver
Cube.js driver for Metabase.

**NOTE: The driver is under development so expect some bugs and missing features. If you find one please create an issue.**

# Features
- **Auto generate data model** from the schema fetched from the Cube.js API
- **Auto create metrics** from the measures. (These metrics are "invalid" when you try to edit them but still usable in queries.)
- Support for **native queries**
- Support for **custom questions**

# Installation
## Get the driver
### Download
Download from the [releases](https://github.com/lili-data/metabase-cubejs-driver/releases).
### Build with Docker
1. Go to the `build` directory
2. Build the docker image: `./image.sh`
3. Build the driver in a container: `./driver.sh`  
   (this will create a `target` directory in the project root)

### Build without Docker
[Use this guide.](https://github.com/tlrobinson/metabase-http-driver/blob/master/README.md#building-the-driver)

## Copy to your Metabase plugins
`cp cubejs.metabase-driver.jar /path/to/metabase/plugins/`  
Note: you have to restart Metabase to load new plugins

# Usage
1. Add and configure your Cube.js "DB" ![Add new DB](./docs/images/config.png)
2. Inspect your Data Model ![Data Model](./docs/images/datamodel.png)
3. Create a query
   - Native ![Native query](./docs/images/nativequery.png)
   - Custom question ![Custom question](./docs/images/customquestion.png)
4. Explore the data ![Results](./docs/images/customresult.png)
# Development
## Roadmap
[v1.0.0](https://github.com/lili-data/metabase-cubejs-driver/milestone/1)

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