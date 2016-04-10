const express = require('express');
const nedb = require('nedb');
const multer = require('multer');
const bodyParser = require('body-parser');
const querystring = require('querystring');
const http = require('http');
const async = require('async');
const fs = require('fs');
var app = express();

var jobs = new nedb({
	filename: './jobs.json',
	autoload: true
});

var slaves = new nedb({
	filename: './slaves.json',
	autoload: true
});

//slaves.update({busy:true}, {$set:{busy:false}});
slaves.remove({});
var frames = new nedb({
	filename: './frames.json',
	autoload: true
});

jobs.ensureIndex({ fieldName: 'filename', unique: true}, function(err){
	if(err){
	console.log(err);
	console.log(":D");
	}
});

slaves.ensureIndex({ fieldName: 'id', unique: true}, function(err){
	if(err){
	console.log(err);
	console.log(":(");
	}
});

var upload = multer({dest:'uploads/'});

app.use(express.static('uploads'));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true}));

app.get('/', function(req, res){
	res.end(JSON.stringify({api_status: 'ok', api_version: '0.0.2'}));
});

app.get('/get_file', function(req, res){
	res.download(__dirname + "/uploads/" + req.query.filename, req.query.filename);
});

app.post('/add_file', upload.single('file'), function(req, res, next){
	req.file.finished = false;
	req.file.requested = [];
	req.file.inprogress = [];
	req.file.result = [];
	req.file.framemap = [];
	jobs.insert(req.file);
	var response = {
		id: req.file.filename
	};
	return res.end(JSON.stringify(response));
});

app.post('/render', function(req, res){
	var framelist = [];
	var start_frame = parseInt(req.body.start_frame, 10);
	var end_frame = parseInt(req.body.end_frame, 10);
	var frame_step = parseInt(req.body.frame_step, 10);
	var i = start_frame;

	async.whilst(
		function() {return i < end_frame;},
		function(callback){
			framelist.push(i);
			i += frame_step;
			callback(null);
		},
		function(err){
			console.log('f: ' + framelist);
			
			jobs.update({ filename:req.body.id }, {$set:{requested:framelist}}, {}, function(){
				dispatch();
			});
		}
	);

/**
	for(i = start_frame; i < end_frame; i += frame_step){
		framelist.push(i);
	}
	console.log("f: " + framelist);
	jobs.update({ filename:req.body.id }, {$set:{requested:framelist}}, {}, function(){
		dispatch();
	});*/	
	res.end("U");	

});

app.post('/register_slave', function(req, res){
	var slave = {
		id: req.body.id,
		jobs: [],
		address:req.body.address,
		port:req.body.port,
		busy:false,
	};

	slaves.insert(slave, function(err, newDoc){
		if(err){
			res.end(JSON.stringify({err:err}));
		}else{
			res.end(JSON.stringify(newDoc));
			dispatch();
		}
	});
});

function dispatch(){
	// while dispatchedFrames < attachedSlaves
	// dispatch a random frame
	console.log("here 2");
	jobs.count({finished:false, $where: function(){return this.requested.length > 0}}, function(err, count){
		if(count <= 0) return;
		console.log("here");
		slaves.find({busy:false}, function(err, slaves_list){
			if(slaves_list.length <= 0 || slaves_list == null|| err) return;
			console.log("sl: " + slaves_list);
			jobs.find({ finished:false, $where: function() {return this.requested.length > 0} }, function(err, jobs_list){
				
				var dispatching = true;
				var i = 0; // job number
				
				num_jobs = jobs_list.length;

				async.whilst( function() { return slaves_list.length > 0 && i < jobs_list.length && dispatching},
					function(next){
					var frame = jobs_list[i].requested.pop();
					var slave = slaves_list.pop();
					jobs_list[i].inprogress.push(frame);
					jobs_list[i].framemap.push({frame: slave.id});
					var id = jobs_list[i].filename;

					// POST /render {frame, id} to slave
					console.log("rendering frame " + frame + " of file " + jobs_list[i].filename + " to slave " + slave.id);				

					dispatchFrame(jobs_list[i].filename, frame, slave.id);
	
					slave.busy = true;
					slave.jobs.push({ id:jobs_list[i].filename, frame:frame });
					slaves.update({ id:slave.id}, slave, {}, function(){
					});
					if(jobs_list[i].requested.length <= 0){
						console.log(i);
						console.log(JSON.stringify(jobs_list[i]));
						jobs.update({ filename:jobs_list[i].filename }, jobs_list[i], {}, function(){
							// updated
						});
						console.log("moving on from job " + jobs_list[i].filename);
						i++;
						if(i >= jobs_list.length) return;
					}
					
					if(slaves_list.length <= 0){
						console.log("-----------------------------");
						dispatching = false;
					}
					next();
					}, 
					function(err){
						if(err) console.log(err);
						console.log("///////////");
						jobs.update({ filename:jobs_list[i].filename }, jobs_list[i], {}, function(){});
					});

				console.log("finished dispatching");

					// update last job if it didn't get updated when switching to the next job
				
			});
		});
	});
}

app.post("/finished", upload.single('file'), function(req, res){
	fs.rename(req.file.path, 'uploads/' + req.file.originalname, (err) => {
		if(err) console.log(err);
	});
	console.log("slave " + req.body.id + " finished");
	slaves.update({id:req.body.id}, {$set:{busy:false}},{},function(){
		dispatch();	
	});
	res.end("U");
});

function dispatchFrame(fileID, frame, slaveID){
	slaves.findOne({id:slaveID}, function(err, doc){
		slaves.update({ id:slaveID }, {$push : {jobs: parseInt(frame, 10)}}, {}, function(){
		});

		var post_data = querystring.stringify({
			'id':fileID,
			'frame':frame,
		});

		var post_options = {
			host: doc.address,
			port: doc.port,
			path: '/render',
			method: 'POST',
			headers: {
				'Content-Type': 'application/x-www-form-urlencoded',
				'Content-Length': Buffer.byteLength(post_data)
			}
		};

		var post_req = http.request(post_options, function(res){
			res.setEncoding('utf8');
			res.on('data', function(chunk){
				console.log('Response: ' + chunk);
			});
		});
		
		post_req.write(post_data);
		post_req.end();
	});
}

app.listen(8080, function(){
	console.log("IT WORKS!!!");
	dispatch();
});






