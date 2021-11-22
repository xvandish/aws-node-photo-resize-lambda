// the code here defines a lambda that reads from SQS, then
// tries to insert the specified record into postgres

// since we are using lambda layers, defining this here instead of a seperate repo,
// as dependancies don't really matter
const AWS = require('aws-sdk');
const { Pool } = require('pg');
const fetch = require('node-fetch')
const parsePgConnectionString = require('pg-connection-string');

// Set the region
AWS.config.update({ region: 'us-west-2' });

// Create an SQS service object
var sqs = new AWS.SQS({ apiVersion: '2012-11-05' });
var queueURL = process.env.IMAGE_META_TO_PG_QUEUE_URL;

const herokuApiKey = process.env.HEROKU_API_KEY;
const herokuPostgresId = process.env.HEROKU_POSTGRES_ID;
let dbPool;

async function initializePgPool() {
    const herokuConfig = await fetch(`https://api.heroku.com/addons/${herokuPostgresId}/config`, {
        headers: {
            'Authorization': `Bearer ${herokuApiKey}`,
            'Accept': 'application/vnd.heroku+json; version=3'
        }
    })
        .then(res => res.json())
        .then(data => data)
        .catch((err) => err)

    if (herokuConfig instanceof Error) {
        return
    }

    const pgConfig = {
        ...parsePgConnectionString(herokuConfig[0].value), // the db string returned by heroku
        min: 0,
        max: 1,
        ssl: {
            rejectUnauthorized: false
        },
        idleTimeoutMillis: 120000,
        connectionTimeoutMillis: 10000,
    }
    dbPool = new Pool(pgConfig)
}

const valueBlockReg = /VALUES\(.*\)/g;

// https://github.com/brianc/node-postgres/issues/957#issuecomment-426852393
// creates n number of VALUES blocks
// ie VALUES ($1, $2, $3) ($1, $2, $3)
function generateValueBlocks(rowCount, columnCount, startAt=1){
    var index = startAt
    return Array(rowCount).fill(0).map(v => `(${Array(columnCount).fill(0).map(v => `$${index++}`).join(", ")})`).join(", ")
}

exports.handler = async (event, context) => {
    // Dont wait for the db dbPool connection to close
    context.callbackWaitsForEmptyEventLoop = false;

    if (!dbPool) {
        console.log('initializing dbPool')
        console.time('get heroku db url and initialize dbPool')
        await initializePgPool();
        console.timeEnd('get heroku db url and initialize dbPool')
        if (!dbPool) {
            return Promise.reject('could not initalize db dbPool')
        }
    } else {
        console.log('dbPool already initialized')
    }

    console.time('connecting to client');
    let client;
    client = await dbPool
        .connect()
        .then((client) => {
            console.log('recieved client succesfully');
            return client;
        })
        .catch((err) => {
            console.error('could not get client from PG dbPool');
            console.error(err.stack);
            return Promise.reject('could not initialize db call');
        });

    if (client instanceof Error) {
        return client;  // return the error
    }

    console.timeEnd('connecting to client');

    // get info from sqs
    let firstQuery = event.Records[0].body
    let insertStatement;
    try {
        firstQuery = JSON.parse(firstQuery);
        insertStatement = firstQuery.text;
    } catch (e) {
        console.error('error parsing body ', e)
        return e;
    }

    // insert into ....... values($1...$n)
    // replace "values($1...$n)"
    const insertsToPerform = event.Records.length;
    const columnCount = firstQuery.values.length;
    
    // [[], [], []]
    const values = event.Records.map((record) => {
        const { body } = record;

        let query;
        try {
            query = JSON.parse(body);
        } catch (e) {
            console.error(err);
        }
        return query.values;
    });

    insertStatement = insertStatement.replace(valueBlockReg, `VALUES ${generateValueBlocks(insertsToPerform, columnCount)}`)

    const query = {
        text: insertStatement,
        values: values.flat()
    }

    console.log({query})

    console.time('db insert');
    const data = await client.query(query)
        .then(res => {
            console.log(res)
            return res;   
        })
        .catch(err => {
            console.error(err.stack);
            return err;
        })
        .finally(() => {
            client.release();
            console.timeEnd('db insert');
        })
    if (data instanceof Error) return data;
    console.log('succesfully inserted batch of size ', event.Records.length)
    return Promise.resolve();
    // connect to pg
}