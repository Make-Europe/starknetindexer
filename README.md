# Typescript: express, mongoose, node-cron

> ### A full template to create complete backend

# 🕹️ Getting started

To get the Node server running locally:

- Clone this repo
- `yarn install` to install all required dependencies
- Configure the `.env` file using `.env.example` template
- `yarn start` to start the local server

# 🧱 Code Overview

## Dependencies

- [expressjs](https://github.com/expressjs/express) - The server for handling and routing HTTP requests
- [mongoose](https://github.com/Automattic/mongoose) - For modeling and mapping MongoDB data to javascript

## Application Structure

- `index.js` - The entry point to our application. This file defines our express server and connects it to MongoDB using mongoose. It also requires the routes and models we'll be using in the application.
- `jobs/` - contains our app's crons to fetch blockchain data hourly and update the morphine db.
- `lib/` - utils functions using apibara and starketjs.
- `controllers/` - routers simple functions.
- `routers/` - This folder contains the route definitions for our API.
- `schema/` - This folder contains the schema definitions for our Mongoose models.
