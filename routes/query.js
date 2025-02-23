const express = require('express');

const router = express.Router();

const queryController = require('../controllers/query');

router.post('/generateResponse', queryController.GetPromptResponse);

module.exports = router;