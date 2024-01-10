import express from 'express';
import bodyParser from 'body-parser';
import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import nodeFetch from 'node-fetch';
import dotenv from 'dotenv';
import skillsService from './services/skills.js';

const skillsServiceObject = new skillsService();
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

      skillsServiceObject.loadSkillsFromFile().then(function (skills){
        for (const item of reduced) {
            getJobByKey(item.jobKey).then(function(response) {
                if (response.length > 0) {
                    return
                }
                skillsServiceObject.findSkillsInJob(item, skills).then(function (matches){
                    item.skills = matches;
                    postJob(item).then(response => response.json()).then(function (response){
                        console.log('posted job ' + item.jobKey + ' : ' + response)
                    }).catch(function (err){
                        console.log('post job error ' + item.jobKey + ' : ' + err.message)
                    })
                })
            }).catch(err => console.log('get job by key error ' + item.jobKey + ' : ' + err.message))
        }

        resp.status(200).send();
    });
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
        more_loc_url: item.moreLocUrl,
        postal: item.postal,
        published_date: item.pubDate,
        remote_work_model_type: item.remoteWorkModelType,
        salary_currency: item.salaryCurrency,
        salary_min: item.salaryMin,
        salary_min_yearly: item.salaryMinYearly,
        salary_max: item.salaryMax,
        salary_max_yearly: item.salaryMaxYearly,
        salary_type: item.salaryType,
        search_term: item.seachTerm,
        snippet: item.snippet,
        skills: item.skills,
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

