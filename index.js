const express = require('express');
const cors = require('cors');
const dotenv = require('dotenv');

dotenv.config();

const PORT = process.env.SERVER_PORT || 3000;

const fileUploadRouter = require('./routes/file_upload');

const app = express();

// configure cors
app.use(cors({
    origin: '*',
    methods: ['GET', 'POST', 'PUT', 'DELETE'],
}));

app.use(express.raw());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.use('/uploads', fileUploadRouter);

app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});