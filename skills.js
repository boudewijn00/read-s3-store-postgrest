import nodeFetch from 'node-fetch';
import dotenv from 'dotenv';
import fs from 'fs';
import skillsService from './services/skills.js';

const skillsServiceObject = new skillsService();

dotenv.config();

const url = process.env.POSTGREST_HOST + '/indeed';
const response = await nodeFetch(url, {
    method: 'GET',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + process.env.POSTGREST_TOKEN },
}).catch(function (err){
    console.log(err)
})

const json = await response.json();

skillsServiceObject.loadSkillsFromFile().then(function (skills){
    for (const job of json) {
        skillsServiceObject.findSkillsInJob(job, skills).then(function (merged){
            patchJob(job, merged).then(function (response){
                console.log(job.job_key + ' ' + response.status)
            }).catch(function (err){
                console.log(err)
            });   
        });
    }
});

async function patchJob(job, merged) {
    const url = process.env.POSTGREST_HOST + '/indeed?job_key=eq.' + job.job_key;
        
    return await nodeFetch(url, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + process.env.POSTGREST_TOKEN },
        body: JSON.stringify({ skills: merged })
    }).catch(function (err){
        console.log(err)
    })
}
