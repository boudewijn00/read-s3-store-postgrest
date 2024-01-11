import fs from 'fs';

class Skills {
    async findSkillsInJob(job, skills) {
        const matches = [];
    
        for (let category in skills) {
            for (let skill in skills[category]) {
                const keyword = skills[category][skill];
                const regex = new RegExp(`\\b${keyword}\\b`, 'i');
                const matchPosition = job.description.search(regex);
                const result = matchPosition >= 0 ? job.description.substring(matchPosition, matchPosition + keyword.length) : null;

                if (result){
                    matches.push(result.toLowerCase());
                }
            }
        }
    
        return matches;
    }

    async loadSkillsFromFile() {
        const skills = fs.readFileSync('./services/skills.json', 'utf8');
    
        return JSON.parse(skills);
    }
}

export default Skills;