# NOTE
CubeJS has an official SQL API now, so try that before using this driver, since it could be more reliable and well supported. You can find more info [here](https://cube.dev/docs/backend/sql).

## Update
Since Cube.js now provides an official way to connect it to Metabase, there is no need for this repo anymore so I'm making it read-only. It was a great journey to learn the "basic" of Clojure, to make this project work and to see how the data flows between the 2 apps.

Thanks to all the people who helped me working on this in any way!


Due to an unfortunate move by someone at my previous company, the "original" repo became private and we lost all the stars, so I leave this screenshot here as a memento :) ![archive](https://user-images.githubusercontent.com/15990318/197162857-9d178d94-9af6-422c-87c0-3a98bbd2d280.png)

---

# metabase-cubejs-driver

[![Latest Release](https://img.shields.io/github/v/release/pyrooka/metabase-cubejs-driver)](https://img.shields.io/github/v/release/pyrooka/metabase-cubejs-driver)
[![GitHub license](https://img.shields.io/badge/license-AGPL-05B8CC.svg)](https://raw.githubusercontent.com/pyrooka/metabase-cubejs-driver/master/LICENSE)

Cube.js driver for Metabase. With this driver you can connect your Cube.js server to Metabase just like a DB.  
Metabase fetches all schemas (cubes) and that's all: you can make queries, filter the results and create beautiful charts and dashboards.

Explanation:

|     Cube.js    |    Metabase    |
|:--------------:|:--------------:|
|     measure    | metric & field |
|    dimension   |      field     |
| time dimension |      field     |

**NOTE**: The driver is under development so expect some bugs and missing features. If you find one please create an issue.

# Features
## Working
- **Auto generate data model** from the schema fetched from the Cube.js API meta endpoint
- **Auto create metrics** from the measures. (These metrics are "invalid" when you try to edit them but still usable in queries.)
- native queries with variables
- custom questions
- filters, orders, limit
## Not working
- Aggregations like sum, count and distinct. This must be done in Cube.js not in Metabase.

# Installation
## Requirements
- Metabase v0.35.0 or newer
## Get the driver
### Download
Download from the [releases](https://github.com/pyrooka/metabase-cubejs-driver/releases).
### Build with Docker
1. Create the docker images: `make docker`
2. Build the driver: `make build`

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
[v1.0.0](https://github.com/pyrooka/metabase-cubejs-driver/milestone/1)

## Testing the driver
1. Create the docker images: `make docker`
2. Start a whole test environment: `make start`

# Contributing
- Any type of contributions are welcomed
- If you find a bug, missing feature or a simple typo just create an issue
- If you can fix/implement it create a pull request

# License
[GNU Affero General Public License v3.0 (AGPL)](https://github.com/pyrooka/metabase-cubejs-driver/blob/master/LICENSE)
