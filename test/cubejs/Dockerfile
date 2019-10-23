FROM node:lts-alpine

# Install cube.js.
RUN npm install -g cubejs-cli

# Create a new app.
RUN cubejs create metabackend -d postgres

# Copy the "test" schemas to the app folder.
COPY schema/* /metabackend/schema/

WORKDIR /metabackend

# Delete the .env file.
RUN rm .env

# Start the app.
CMD [ "npm", "run", "dev" ]