import fs from 'fs';

class Skills {
    async findSkillsInJob(job, skills) {
        const matches = [];
    
        for (let category in skills) {
            for (let skill in skills[category]) {
                const regex = new RegExp(skills[category][skill], 'gi');
                const result = job.description.match(regex);
                if (result) {
                    const unique = result ? Array.from(new Set(result.map(match => match.toLowerCase()))) : [];
                    matches.push(unique);
                }
            }
        }
    
        const merged = [].concat.apply([], matches);
    
        return merged;
    }

    async loadSkillsFromFile() {
        const skills = fs.readFileSync('./services/skills.json', 'utf8');
    
        return JSON.parse(skills);
    }
}

export default Skills;