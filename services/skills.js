import fs from 'fs';

class Skills {
    async findSkillsInJob(job, skills) {
        const matches = [];
    
        for (let category in skills) {
            for (let skill in skills[category]) {
                var keyword = skills[category][skill];
                var regex = new RegExp(`\\b${keyword}\\b`, 'i');

                // Find the position of the first match in the searchString
                var matchPosition = job.description.search(regex);

                // Return the result
                var result = matchPosition >= 0 ? job.description.substring(matchPosition, matchPosition + keyword.length) : null;
                
                if (result){
                    matches.push(result);
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