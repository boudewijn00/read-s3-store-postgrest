import express from 'express';
import bodyParser from 'body-parser';
import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import nodeFetch from 'node-fetch';
import dotenv from 'dotenv';

dotenv.config();
const app = express();

app.get('/', (req, res) => {
    res.send('Hello World!');
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

    const { Body } = await client.send(new GetObjectCommand({
        Bucket: s3.bucket.name,
        Key: s3.object.key,
    }));

    const contents = await Body.transformToString();
    const json = await JSON.parse(contents);

    const map = new Map();
    const reduced = json.reduce((acc, item) => {
        if (!map.has(item['jobKey'])) {
          map.set(item['jobKey'], true);
          acc.push(item);
        }

        return acc;
      }, []);

    console.log(reduced.length)

    for (const item of json) {
        getJobByKey(item.jobKey).then(function(response) {
            if (response.length === 0) {
                postJob(item).then(function (response){
                    console.log(response)
                }).catch(function (err){
                    console.log(err)
                })
            }
        }).catch(err => console.log(err))
    }

    resp.status(200).send();
}

async function getJobByKey(key) {
    const url = process.env.POSTGREST_HOST + '/indeed?job_key=eq.' + key
    const response = await nodeFetch(url, {
        method: 'GET',
        headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + process.env.POSTGREST_TOKEN },
    }).catch(err => console.log(err))
    
    return await response.json()
}

async function postJob(item) {
    const payload = {
        apply_count: item.applyCount,
        company: item.company,
        company_rating: item.companyRating,
        company_review_count: item.companyReviewCount,
        company_overview_link: item.companyOverviewLink,
        company_id_encrypted: item.companyIdEncrypted,
        country: item.country,
        created_date: item.createDate,
        description: item.description,
        expired: item.expired,
        formatted_relative_time: item.formattedRelativeTime,
        job_key: item.jobKey,
        job_card_requirements_model: item.jobCardRequirementsModel,
        location: item.location,
        min_salary_yearly: item.minSalaryYearly,
        max_salary_yearly: item.maxSalaryYearly,
        more_loc_url: item.moreLocUrl,
        postal: item.postal,
        published_date: item.pubDate,
        remote_work_model_type: item.remoteWorkModelType,
        salary_min: item.extractedSalary ? item.extractedSalary.min > 0 ? item.extractedSalary.min : null : null,
        salary_max: item.extractedSalary ? item.extractedSalary.max > 0 ? item.extractedSalary.max : null : null,
        salary_type: item.extractedSalary ? item.extractedSalary.type : null,
        salary_currency: item.salarySnippet ? item.salarySnippet.currency : null,
        search_term: item.seachTerm,
        snippet: item.snippet,
        state: item.state,
        title: item.title
    }

    const stringify = JSON.stringify(payload)
    const url = process.env.POSTGREST_HOST + '/indeed'
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
    }
);

