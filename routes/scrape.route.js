const express = require('express');
const router = express.Router();

//Load Controllers
const {
    newScrapeController,
} = require('../controllers/scrape.controller.js')


router.post('/scrape/new', newScrapeController);

module.exports = router;