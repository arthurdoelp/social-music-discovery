// const Sequelize = require('sequelize');
// const Op = Sequelize.Op;
const spawn = require("child_process").spawn;
// const axios = require("axios");

exports.newScrapeController = (req, res) => {
    console.log("Controller is connected!");

    const pythonProcess = spawn('python', ["python/scrape.py"]);

    pythonProcess.stdout.on('data', (data) => {
        // var buf = JSON.stringify(data.toString());
        // console.log(buf)
        // var formatted_data = JSON.parse(buf);
        // console.log(formatted_data)

        res.json({ data: "Success!" })
    })
}