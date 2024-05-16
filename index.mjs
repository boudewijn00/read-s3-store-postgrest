import express from 'express';
import bodyParser from 'body-parser';
import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import nodeFetch from 'node-fetch';
import dotenv from 'dotenv';

dotenv.config();
const app = express();

app.get('/', (req, res) => {
    res.send('read-s3-store-postgrest servce is running');
});

app.post('/', bodyParser.text(), handleSNSMessage);

async function handleSNSMessage(req, resp) {
    const body = JSON.parse(req.body);

    if (body.Type === 'SubscriptionConfirmation') {
        const url = body.SubscribeURL;
        const response = await nodeFetch(url, {
            method: 'GET',
        }).catch(err => console.log(err))

        return resp.status(200).send();
    }

    const message = JSON.parse(body.Message);
    const record = message.Records[0];
    const s3 = record.s3;

    const client = new S3Client({
        region: 'eu-central-1',
        credentials: {
            accessKeyId: process.env.AWS_ACCESS_KEY_ID,
            secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
        },
    });

    const bucket = s3.bucket.name;
    const key = s3.object.key;
    const decodedKey = decodeURIComponent(key.replace(/\+/g, ' '));
    const { Body } = await client.send(new GetObjectCommand({
        Bucket: bucket,
        Key: decodedKey,
    }));

    const contents = await Body.transformToString();
    const json = await JSON.parse(contents);
    let results = json;
    let table = '';

    if (key.includes('gumroad')) {
        table = 'gumroad';
    } else if (key.includes('leanpub')) {
        table = 'leanpub';
    } else if (key.includes('indeed')) {
        const map = new Map();
        results = json.reduce((acc, item) => {
            if (!map.has(item['external_id'])) {
                map.set(item['external_id'], true);
                acc.push(item);
            }

            return acc;
        }, []);

        table = 'indeed';
    } else {
        resp.status(400).send(); 
    }

    for (const item of results) {
        getByExternalId(table, item.external_id).then(function(response) {
            if (response.length > 0) {
                return
            }

            postItem(table, item).then(function (){
                console.log('posted item response ' + item.external_id)
            }).catch(function (){
                console.log('post item error ' + item.external_id)
            })
        }).catch(err => console.log(err))
    }

    resp.status(200).send();
}

async function getByExternalId(table, id) {
    const url = process.env.POSTGREST_HOST + '/' + table + '?external_id=eq.' + id
    const response = await nodeFetch(url, {
        method: 'GET',
        headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + process.env.POSTGREST_TOKEN },
    }).catch(err => console.log(err))
    
    return await response.json()
}

async function postItem(table, item) {
    const stringify = JSON.stringify(item)
    const url = process.env.POSTGREST_HOST + '/' + table
    const response = await nodeFetch(url, {
        method: 'POST',
        body: stringify,
        headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + process.env.POSTGREST_TOKEN },
    }).catch(function (err){
        console.log(err)
    })

    return response
}

app.listen(3000, () => {
    console.log('Server is listening on port 3000');
});

