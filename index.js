const express = require('express');
const bodyParser = require('body-parser');
const mysql = require('mysql');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const csv = require('fast-csv');
const { error } = require('console');
const cors = require('cors');

const app = express();

app.use(cors());

app.use(bodyParser.urlencoded({extended:false}));
app.use(bodyParser.json());

// multer config
let storage = multer.diskStorage({
    destination:(req,file,callback)=>{
        callback(null,"./uploads/")
    },
    filename:(req,file,callback)=>{
        callback(null,file.fieldname + "-" + Date.now() + path.extname(file.originalname));
    }
})

let upload = multer({
    storage:storage
});




const pool = mysql.createPool({
    host:'62.72.28.52',
    user:'u276789778_stocks',
    password:'123@Mango',
    database:'u276789778_stocks',
})



// broad POST request
app.post('/upload-broad', upload.single('file'), (req, res) => {
    if (!req.file) {
        return res.status(400).send('No file uploaded.');
    }
    console.log('File uploaded:', req.file);
    uploadBroad(__dirname + "/uploads/" + req.file.filename,req.file.originalname,(error)=>{
        if(error){
            return res.status(5000).json({error:'error uploading csv file'});
        }
        res.json({message:'file uploaded successfully'});
    }); 
});

//strategy POST request
app.post('/upload-strategy', upload.single('file'), (req, res) => {
    if (!req.file) {
        return res.status(400).send('No file uploaded.');
    }
    console.log('File uploaded:', req.file);
    uploadStrategy(__dirname + "/uploads/" + req.file.filename,req.file.originalname,(error)=>{
        if(error){
            return res.status(5000).json({error:'error uploading csv file'});
        }
        res.json({message:'file uploaded successfully'});
    }); 
});


//thematic POST request
app.post('/upload-thematic', upload.single('file'), (req, res) => {
    if (!req.file) {
        return res.status(400).send('No file uploaded.');
    }
    console.log('File uploaded:', req.file);
    uploadThematic(__dirname + "/uploads/" + req.file.filename,req.file.originalname,(error)=>{
        if(error){
            return res.status(5000).json({error:'error uploading csv file'});
        }
        res.json({message:'file uploaded successfully'});
    }); 
});

// sector POST request
app.post('/upload-sector', upload.single('file'), (req, res) => {
    if (!req.file) {
        return res.status(400).send('No file uploaded.');
    }
    console.log('File uploaded:', req.file);
    uploadSector(__dirname + "/uploads/" + req.file.filename,req.file.originalname,(error)=>{
        if(error){
            return res.status(5000).json({error:'error uploading csv file'});
        }
        res.json({message:'file uploaded successfully'});
    }); 
});



// all indices data POST request
app.post('/upload-alldata', upload.single('file'), (req, res) => {
    if (!req.file) {
        return res.status(400).send('No file uploaded.');
    }
    console.log('File uploaded:', req.file);
    uploadAllData(__dirname + "/uploads/" + req.file.filename,req.file.originalname,(error)=>{
        if(error){
            return res.status(5000).json({error:'error uploading csv file'});
        }
        res.json({message:'file uploaded successfully'});
    }); 
});


//get all uploaded files data
app.get('/uploadedFiles-data',(req,res)=>{
    try{
        pool.getConnection((err,connection)=>{
            if(err){
                console.log('Error getting database connection:', err);
                return res.status(500).json({error:'Internal Server Error'});
            }else{
                const query = 'SELECT * FROM file_data';
                connection.query(query,(error,results)=>{
                    if(error){
                        console.log('Error executing the query',error);
                        return res.status(500).json({ error: 'Internal Server Error' });
                    }
                    res.status(200).json({results});
                })
            }
        })
    }catch(e){
        console.log('error getting files data',e);
        res.status(500).json({ error: 'Internal Server Error' });
    }
})


//get all subscribers data
app.get('/subscribers-data',(req,res)=>{
    try{
        pool.getConnection((err,connection)=>{
            if(err){
                console.log('Error getting database connection:', err);
                return res.status(500).json({error:'Internal Server Error'});
            }else{
                const query = 'SELECT * FROM users WHERE is_subscribed = true';
                connection.query(query,(error,results)=>{
                    if(error){
                        console.log('Error executing the query',error);
                        return res.status(500).json({ error: 'Internal Server Error' });
                    }
                    res.status(200).json({results});
                })
            }
        })
    }catch(e){
        console.log('error getting files data',e);
        res.status(500).json({ error: 'Internal Server Error' });
    }
})



//formated date
function formatTimestamp(timestamp) {
    let date = new Date(timestamp);
    // Get day, month, and year from the Date object
    let day = date.getDate().toString().padStart(2, '0'); 
    let month = (date.getMonth() + 1).toString().padStart(2, '0'); 
    let year = date.getFullYear(); // Get full year
    // Format the date as dd/mm/yyyy
    let formattedDate = `${day}/${month}/${year}`;

    return formattedDate;
}




// all indices data function
function uploadAllData(path,fileName, callback) {
    let stream = fs.createReadStream(path);
    let csvData = [];
    let fileStream = csv.parse().on('data', function (data) {
        csvData.push(data);
    }).on('end', function () {
        csvData.shift();
        pool.getConnection((error, connection) => {
            if (error) {
                console.log(error);
                if(error.code === 'ETIMEDOUT'){
                    setTimeout(() => {
                        uploadAllData(path, fileName, callback);
                    }, 5000); // Retry after 5 seconds
                    return;
                } else {
                    // Handle other errors
                    callback(error);
                    return;
                }
            } else {
                let date = Date.now();
                let formattedDate = formatTimestamp(date); 
                let name = 'Stock & Heat';
                let fileData=[name,formattedDate,fileName]

                    let query = "INSERT INTO heat_map_data VALUES ?";
                    let fileQuery = 'INSERT INTO file_data (category, dates, file_name) VALUES (?)';


                    connection.query(query, [csvData], (insertError, insertResult) => {
                        if (insertError) {
                            console.error('Error uploading new data', insertError);
                            connection.release();
                            return callback(insertError);
                        } else {
                            console.log('Data uploaded successfully');
                            console.log(csvData)

                            connection.query(fileQuery, [fileData], (fileInsertError, fileInsertResult) => {
                                if (fileInsertError) {
                                    console.error('Error uploading file data', fileInsertError);
                                    connection.release();
                                    return callback(fileInsertError);
                                } else {
                                    console.log('File data uploaded successfully');
                                    connection.release();
                                    return callback(null);
                                }
                            });
                        }
                    });
            }
        });
    });
    stream.pipe(fileStream);
}




//broad function
function uploadBroad(path,fileName, callback) {
    let stream = fs.createReadStream(path);
    let csvData = [];
    let headers = []; 
    let fileStream = csv.parse().on('data', function (data) {
        csvData.push(data); 
    }).on('end', function () {
        const headerRow = csvData.shift(); 
        headers = headerRow.slice(1).map(header => header.trim()); 
        
        pool.getConnection((error, connection) => {
            if (error) {
                console.log(error);
            } else {
                // Fetch column names from the database table
                connection.query('SHOW COLUMNS FROM broadheatmap', (columnError, columnResult) => {
                    if (columnError) {
                        console.error('Error fetching column names', columnError);
                        connection.release();
                        return callback(columnError);
                    }

                    const dbColumnNames = columnResult.map(column => column.Field); 

                    // Iterate through each header in the CSV file
                    headers.forEach(header => {
                        if (dbColumnNames.includes(header)) {
                            const index = headers.indexOf(header);
                            csvData.forEach(row => {
                                let valueToUpdate = row[index + 1];
                                const commonColumnName = row[0];
                                let date = Date.now();
                                let formattedDate = formatTimestamp(date); 
                                let name = 'Broad';
                                let fileData=[name,formattedDate,fileName]
            
                                let fileQuery = 'INSERT INTO file_data (category, dates, file_name) VALUES (?)';

                                // Fetch the current value of the column from the database
                                connection.query(`SELECT ?? FROM broadheatmap WHERE ${dbColumnNames[0]} = ?`, [header, commonColumnName], (selectError, selectResult) => {
                                    if (selectError) {
                                        console.error('Error fetching previous value', selectError);
                                    } else {
                                        let previousValue;
                                        if(selectResult[0]){
                                            previousValue = selectResult[0][header] || '';
                                        }
                                        console.log('PPPPrevious value',previousValue);

                                        const average = (parseFloat(valueToUpdate)+previousValue)/2;
                                        
                                        // Update the column with the new value
                                        const query = `UPDATE broadheatmap SET ?? = ? WHERE ${dbColumnNames[0]} = ?`; 
                                        connection.query(query, [header, average, commonColumnName], (updateError, updateResult) => {
                                            if (updateError) {
                                                console.error('Error updating row', updateError);
                                            } else {
                                                console.log(' updated successfully');
                                                
                                                connection.query(fileQuery, [fileData], (fileInsertError, fileInsertResult) => {
                                                    if (fileInsertError) {
                                                        console.error('Error uploading file data', fileInsertError);
                                                        connection.release();
                                                        return callback(fileInsertError);
                                                    } else {
                                                        console.log('File data uploaded successfully');
                                                        connection.release();
                                                        return callback(null);
                                                    }
                                                });
                                            }
                                        });
                                    }
                                });
                            });
                        }
                    });

                    connection.release();
                    return callback(null);
                });
            }
        });
    });
    stream.pipe(fileStream);
}





//strategy function
function uploadStrategy(path,fileName, callback) {
    let stream = fs.createReadStream(path);
    let csvData = [];
    let headers = []; 
    let fileStream = csv.parse().on('data', function (data) {
        csvData.push(data); 
    }).on('end', function () {
        const headerRow = csvData.shift(); 
        headers = headerRow.slice(1).map(header => header.trim()); 
        console.log(`**********${headers}`);
        
        pool.getConnection((error, connection) => {
            if (error) {
                console.log(error);
            } else {
                // Fetch column names from the database table
                connection.query('SHOW COLUMNS FROM strategyheatmap', (columnError, columnResult) => {
                    if (columnError) {
                        console.error('Error fetching column names', columnError);
                        connection.release();
                        return callback(columnError);
                    }

                    const dbColumnNames = columnResult.map(column => column.Field); 

                    // Iterate through each header in the CSV file
                    headers.forEach(header => {
                        if (dbColumnNames.includes(header)) {
                            const index = headers.indexOf(header);
                            csvData.forEach(row => {
                                let valueToUpdate = row[index + 1];
                                console.log('csv data', valueToUpdate);

                                const commonColumnName = row[0];
                                console.log('col names of the csv file', commonColumnName);

                                let date = Date.now();
                                let formattedDate = formatTimestamp(date); 
                                let name = 'Strategy';
                                let fileData=[name,formattedDate,fileName]
            
                                let fileQuery = 'INSERT INTO file_data (category, dates, file_name) VALUES (?)';

                                // Fetch the current value of the column from the database
                                connection.query(`SELECT ?? FROM strategyheatmap WHERE ${dbColumnNames[0]} = ?`, [header, commonColumnName], (selectError, selectResult) => {
                                    if (selectError) {
                                        console.error('Error fetching previous value', selectError);
                                    } else {
                                        // Calculate the new value by adding the previous value and valueToUpdate
                                        let previousValue;
                                        if(selectResult[0]){
                                            previousValue = selectResult[0][header] || ''; 
                                        }
                    
                                        const average = (parseFloat(valueToUpdate)+previousValue)/2;

                                        // Update the column with the new value
                                        const query = `UPDATE strategyheatmap SET ?? = ? WHERE ${dbColumnNames[0]} = ?`; 
                                        connection.query(query, [header, average, commonColumnName], (updateError, updateResult) => {
                                            if (updateError) {
                                                console.error('Error updating row', updateError);
                                            } else {
                                                console.log(' updated successfully');
                                                
                                                connection.query(fileQuery, [fileData], (fileInsertError, fileInsertResult) => {
                                                    if (fileInsertError) {
                                                        console.error('Error uploading file data', fileInsertError);
                                                        connection.release();
                                                        return callback(fileInsertError);
                                                    } else {
                                                        console.log('File data uploaded successfully');
                                                        connection.release();
                                                        return callback(null);
                                                    }
                                                });
                                            }
                                        });
                                    }
                                });
                            });
                        }
                    });

                    connection.release();
                    return callback(null);
                });
            }
        });
    });
    stream.pipe(fileStream);
}


//thematic function
function uploadThematic(path,fileName, callback) {
    let stream = fs.createReadStream(path);
    let csvData = [];
    let headers = []; 
    let fileStream = csv.parse().on('data', function (data) {
        csvData.push(data); 
    }).on('end', function () {
        const headerRow = csvData.shift(); 
        headers = headerRow.slice(1).map(header => header.trim()); 
        console.log(`**********${headers}`);
        
        pool.getConnection((error, connection) => {
            if (error) {
                console.log(error);
            } else {
                // Fetch column names from the database table
                connection.query('SHOW COLUMNS FROM thematicheatmap', (columnError, columnResult) => {
                    if (columnError) {
                        console.error('Error fetching column names', columnError);
                        connection.release();
                        return callback(columnError);
                    }

                    const dbColumnNames = columnResult.map(column => column.Field); 

                    // Iterate through each header in the CSV file
                    headers.forEach(header => {
                        if (dbColumnNames.includes(header)) {
                            const index = headers.indexOf(header);
                            csvData.forEach(row => {
                                let valueToUpdate = row[index + 1];
                                console.log('csv data', valueToUpdate);

                                const commonColumnName = row[0];
                                console.log('col names of the csv file', commonColumnName);

                                let date = Date.now();
                                let formattedDate = formatTimestamp(date); 
                                let name = 'Thematic';
                                let fileData=[name,formattedDate,fileName]
            
                                let fileQuery = 'INSERT INTO file_data (category, dates, file_name) VALUES (?)';

                                // Fetch the current value of the column from the database
                                connection.query(`SELECT ?? FROM thematicheatmap WHERE ${dbColumnNames[0]} = ?`, [header, commonColumnName], (selectError, selectResult) => {
                                    if (selectError) {
                                        console.error('Error fetching previous value', selectError);
                                    } else {
                                       
                                        let previousValue;
                                        if(selectResult[0]){
                                            previousValue = selectResult[0][header] || '';
                                        }
                                        console.log('PPPPrevious value',previousValue);

                                    
                                        const average = (parseFloat(valueToUpdate)+previousValue)/2;

                                        console.log('average',average);

                                        // Update the column with the new value
                                        const query = `UPDATE thematicheatmap SET ?? = ? WHERE ${dbColumnNames[0]} = ?`; 
                                        connection.query(query, [header, average, commonColumnName], (updateError, updateResult) => {
                                            if (updateError) {
                                                console.error('Error updating row', updateError);
                                            } else {
                                                console.log(' updated successfully');


                                                connection.query(fileQuery, [fileData], (fileInsertError, fileInsertResult) => {
                                                    if (fileInsertError) {
                                                        console.error('Error uploading file data', fileInsertError);
                                                        connection.release();
                                                        return callback(fileInsertError);
                                                    } else {
                                                        console.log('File data uploaded successfully');
                                                        connection.release();
                                                        return callback(null);
                                                    }
                                                });
                                            }
                                        });
                                    }
                                });
                            });
                        }
                    });

                    connection.release();
                    return callback(null);
                });
            }
        });
    });
    stream.pipe(fileStream);
}


//sector function
function uploadSector(path,fileName, callback) {
    let stream = fs.createReadStream(path);
    let csvData = [];
    let headers = []; 
    let fileStream = csv.parse().on('data', function (data) {
        csvData.push(data); 
    }).on('end', function () {
        const headerRow = csvData.shift(); 
        headers = headerRow.slice(1).map(header => header.trim()); 
        console.log(`**********${headers}`);
        
        pool.getConnection((error, connection) => {
            if (error) {
                console.log(error);
            } else {
                // Fetch column names from the database table
                connection.query('SHOW COLUMNS FROM sectorheatmap', (columnError, columnResult) => {
                    if (columnError) {
                        console.error('Error fetching column names', columnError);
                        connection.release();
                        return callback(columnError);
                    }

                    const dbColumnNames = columnResult.map(column => column.Field); 

                    // Iterate through each header in the CSV file
                    headers.forEach(header => {
                        if (dbColumnNames.includes(header)) {
                            const index = headers.indexOf(header);
                            csvData.forEach(row => {
                                let valueToUpdate = row[index + 1];
                                console.log('csv data', valueToUpdate);

                                const commonColumnName = row[0];
                                console.log('col names of the csv file', commonColumnName);

                                let date = Date.now();
                                let formattedDate = formatTimestamp(date); 
                                let name = 'Sector';
                                let fileData=[name,formattedDate,fileName]
            
                                let fileQuery = 'INSERT INTO file_data (category, dates, file_name) VALUES (?)';

                                // Fetch the current value of the column from the database
                                connection.query(`SELECT ?? FROM sectorheatmap WHERE ${dbColumnNames[0]} = ?`, [header, commonColumnName], (selectError, selectResult) => {
                                    if (selectError) {
                                        console.error('Error fetching previous value', selectError);
                                    } else {
                                        // Calculate the new value by adding the previous value and valueToUpdate
                                        let previousValue;
                                        if(selectResult[0]){
                                            previousValue = selectResult[0][header] || ''; // Set previousValue to the fetched value or 0 if undefined
                                        }
                                        console.log('PPPPrevious value',previousValue);

                                        const average = (parseFloat(valueToUpdate)+previousValue)/2;

                                        console.log('average',average);

                                        // Update the column with the new value
                                        const query = `UPDATE sectorheatmap SET ?? = ? WHERE ${dbColumnNames[0]} = ?`; 
                                        connection.query(query, [header, average, commonColumnName], (updateError, updateResult) => {
                                            if (updateError) {
                                                console.error('Error updating row', updateError);
                                            } else {
                                                console.log(' updated successfully');
                                                
                                                connection.query(fileQuery, [fileData], (fileInsertError, fileInsertResult) => {
                                                    if (fileInsertError) {
                                                        console.error('Error uploading file data', fileInsertError);
                                                        connection.release();
                                                        return callback(fileInsertError);
                                                    } else {
                                                        console.log('File data uploaded successfully');
                                                        connection.release();
                                                        return callback(null);
                                                    }
                                                });
                                            }
                                        });
                                    }
                                });
                            });
                        }
                    });

                    connection.release();
                    return callback(null);
                });
            }
        });
    });
    stream.pipe(fileStream);
}



//enable CORS
app.use((req,res,next)=>{
    res.header('Access-control-allow-origin','*');
    res.header('Access-contorl-allow-headers','origin, x-requested-with,content-type, accept');
    next();
})

const PORT = process.env.PORT || 4000;

app.listen(PORT, ()=>{
    console.log(`App is listening on port ${PORT}`);
})










