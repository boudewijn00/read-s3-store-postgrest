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

    const bucket = s3.bucket.name;
    const key = s3.object.key;
    const decodedKey = decodeURIComponent(key.replace(/\+/g, ' '));
    const { Body } = await client.send(new GetObjectCommand({
        Bucket: bucket,
        Key: decodedKey,
    }));

    const contents = await Body.transformToString();
    const json = await JSON.parse(contents);

    if (key.includes('gumroad')) {
        for (const item of json) {
            getGumroadProductById(item.id).then(function(response) {
                if (response.length > 0) {
                    return
                }

                postGumroadProduct(item).then(function (response){
                    console.log('posted gumroad product response ' + item.id)
                }).catch(function (err){
                    console.log('post gumroad product error ' + item.id)
                })
            }).catch(err => console.log(err))
        }

        resp.status(200).send();
    } else if (key.includes('leanpub')) {
        for (const item of json) {
            getLeanpubById(item.id).then(function(response) {
                if (response.length > 0) {
                    return
                }

                postLeanpub(item).then(function (response){
                    console.log('posted leanpub response ' + item.id)
                }).catch(function (err){
                    console.log('post leanpub error ' + item.id)
                })
            }).catch(err => console.log(err))
        }

        resp.status(200).send();
    } else if (key.includes('indeed')) {
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
                            console.log('posted job response ' + item.jobKey + ' : ' + response.message)
                        }).catch(function (err){
                            console.log('post job error ' + item.jobKey + ' : ' + err.message)
                        })
                    })
                }).catch(err => console.log('get job by key error ' + item.jobKey + ' : ' + err.message))
            }

            resp.status(200).send();
        });
    } else {
        resp.status(200).send();
    }

}

async function getLeanpubById(id) {
    const url = process.env.POSTGREST_HOST + '/leanpubid=eq.' + id
    const response = await nodeFetch(url, {
        method: 'GET',
        headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + process.env.POSTGREST_TOKEN },
    }).catch(err => console.log(err))
    
    return await response.json()
}

async function getGumroadProductById(id) {
    const url = process.env.POSTGREST_HOST + '/gumroad?product_id=eq.' + id
    const response = await nodeFetch(url, {
        method: 'GET',
        headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + process.env.POSTGREST_TOKEN },
    }).catch(err => console.log(err))
    
    return await response.json()
}

async function getJobByKey(key) {
    const url = process.env.POSTGREST_HOST + '/indeed?job_key=eq.' + key
    const response = await nodeFetch(url, {
        method: 'GET',
        headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + process.env.POSTGREST_TOKEN },
    }).catch(err => console.log(err))
    
    return await response.json()
}

async function postLeanpub(item) {
    const stringify = JSON.stringify(item)
    const url = process.env.POSTGREST_HOST + '/leanpub'
    const response = await nodeFetch(url, {
        method: 'POST',
        body: stringify,
        headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + process.env.POSTGREST_TOKEN },
    }).catch(function (err){
        console.log(err)
    })

    return response
}

async function postGumroadProduct(item) {
    const payload = {
        "attributes": item.attributes,
        "bundle_products": item.bundle_products,
        "currency_code": item.currency_code,
        "description": item.description_html,
        "free_trial": item.free_trial,
        "price_cents": item.price_cents,
        "product_id": item.id,
        "product_name": item.name,
        "rating_counts": item.rating_counts,
        "refund_policy": item.refund_policy,
        "sales_count": item.sales_count,
        "seller_name": item.seller_name,
        "seller_id": item.seller_id,
        "seller_profile_url": item.seller_profile_url,
        "summary": item.summary,
        "thumbnail_url": item.thumbnail_url,
        "url": item.url
    }

    const stringify = JSON.stringify(payload)
    const url = process.env.POSTGREST_HOST + '/gumroad'
    const response = await nodeFetch(url, {
        method: 'POST',
        body: stringify,
        headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + process.env.POSTGREST_TOKEN },
    }).catch(function (err){
        console.log(err)
    })

    return response
}

async function postJob(item) {
    const payload = {
        apply_count: item.applyCount,
        company: item.company,
        company_rating: item.companyRating,
        company_review_count: item.companyReviewCount,
        company_overview_link: item.companyOverviewLink,
        company_id_encrypted: item.companyIdEncrypted || '',
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
        salary_min: Math.trunc(item.salaryMin),
        salary_min_yearly: Math.trunc(item.salaryMinYearly),
        salary_max: Math.trunc(item.salaryMax),
        salary_max_yearly: Math.trunc(item.salaryMaxYearly),
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

