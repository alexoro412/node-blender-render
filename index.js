const express = require('express');
const nedb = require('nedb');
const multer = require('multer');
const bodyParser = require('body-parser');
const querystring = require('querystring');
const http = require('http');
const async = require('async');
const fs = require('fs');
const needle = require('needle');
const zpad = require('zpad');
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
slaves.remove({}, {multi:true});
//var frames = new nedb({
//	filename: './frames.json',
//	autoload: true
//});

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

function doubleCheck(id){
	console.log("checking " + id);
	jobs.findOne({filename:id}, function(err, newDoc){
		//for(var i = newDoc.requested[0]; i < newDoc.requested[-1]; i+= newDoc.requested[1]-newDoc.requested[0]){
	//	if(newDoc.inprogress.length > 0) return;
		var i = 0;
		console.log("found " + id);
		async.whilst(
			function(){
				return i < newDoc.requested.length;
			},
			function(callback){
				var frame = newDoc.requested[i];
				var path = "./uploads/" + newDoc.filename + "_" + zpad(frame, 4) + ".png";
					try{
					fs.accessSync(path, fs.F_OK, (err) => {
						if(err){
							console.err("there was error: ", err);
							db.update({filename:id}, {$push:{requested_poppable:frame}});
							console.log("frame " + frame + " not completed... oops... try again");
						}else{
							console.log("fine");
						}
					});
					}catch(e){
						jobs.update({filename:id}, {$push:{requested_poppable:frame}});
						console.log("frame " + frame + " for job " + id + " must be retried");
					}
				i++;
				callback(null);
			},
			function(err){
				dispatch();
			}
		);			
	});
}

app.get('/', function(req, res){
	res.end(JSON.stringify({api_status: 'ok', api_version: '0.0.2'}));
});

app.get('/get_file', function(req, res){
	res.download(__dirname + "/uploads/" + req.query.filename, req.query.filename);
});

app.post('/add_file', upload.single('file'), function(req, res, next){
	req.file.finished = false;
	req.file.requested = [];
	req.file.requested_poppable = [];
	req.file.inprogress = [];
	req.file.result = [];
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
			
			jobs.update({ filename:req.body.id }, {$push:{requested:{$each:framelist}, requested_poppable:{$each:framelist}}}, {}, function(){
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

function slaveConnect(id, online){
	slaves.update({id:id}, {$set:{online:online}}, {}, function(err){
		if(err) console.log(err);
	});
}

app.post('/register_slave', function(req, res){
	var slave = {
		id: req.body.id,
		jobs: [],
		address:req.body.address,
		port:req.body.port,
		busy:false,
		online:true,
	};

	slaves.insert(slave, function(err, newDoc){
		if(err){
			// TODO if uniqueViolated, simply update the slave
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
																												// some jobs left to dispatch
	jobs.count({finished:false, $where: function(){return (this.inprogress.length + this.result.length) < (this.requested.length)}}, function(err, count){
		console.log("jobs count: " + count);
		if(count <= 0) return;
		slaves.find({busy:false}, function(err, slaves_list){
			console.log("slaves count: " + slaves_list.length);
			if(slaves_list.length <= 0 || slaves_list == null|| err) return;
			jobs.find({ finished:false, $where: function() {return (this.inprogress.length + this.result.length) < (this.requested.length)} }, function(err, jobs_list){
				
				var dispatching = true;
				var i = 0; // job number
				
				num_jobs = jobs_list.length;

				async.whilst( function() { return slaves_list.length > 0 && i < jobs_list.length && dispatching},
					function(next){
					var frame = jobs_list[i].requested_poppable.pop();
					var slave = slaves_list.pop();
					jobs_list[i].inprogress.push(frame);
					var id = jobs_list[i].filename;

					// POST /render {frame, id} to slave
					console.log("rendering frame " + frame + " of file " + jobs_list[i].filename + " to slave " + slave.id);				

					dispatchFrame(jobs_list[i].filename, frame, slave.id);
	
					slave.busy = true;
					slave.jobs.push({ id:jobs_list[i].filename, frame:frame });
					slaves.update({ id:slave.id}, slave, {}, function(){
					});
					if(jobs_list[i].requested_poppable.length <= 0){
						console.log(i);
						console.log(JSON.stringify(jobs_list[i]));
						var id = jobs_list[i].filename;
						jobs.update({ filename:jobs_list[i].filename }, jobs_list[i], {}, function(){
							// updated
							doubleCheck(id);
						});
						console.log("moving on from job " + jobs_list[i].filename);
						i++;
						if(i >= jobs_list.length){
							i--;
							return;
						}
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
						if(jobs_list.length > 0)
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
		jobs.findOne({filename:req.body.job_id}, function(err, j){
			var indexOfFinishedFrame = j.inprogress.indexOf(req.body.frame);
			j.result.push(j.inprogress.splice(indexOfFinishedFrame, 1)[0]);
			console.log("moved frame to result " + req.body.frame);
			jobs.update({filename:req.body.job_id}, j, {}, function(err){
				dispatch();
				doubleCheck(req.body.job_id);
			});
		});
		//dispatch();
		//doubleCheck(req.body.job_id);	
	});
	// TODO
	res.end("U");
});

function dispatchFrame(fileID, frame, slaveID){
	slaves.findOne({id:slaveID}, function(err, doc){	
		var data = {
			id:fileID,
			frame:frame,
		};
		console.log(doc.address + ":" + doc.port + " | " + slaveID)
		needle.post(doc.address + ":" + doc.port + "/render", data, {}, function(err, resp){
			if(err){
				console.log(err);
				slaves.update({ id:slaveID }, {$set : {online:false}}, {}, function(){});
				jobs.update({ filename:fileID }, {$push : {requested:frame, requested_poppable:frame}});
			}
		});
	});
}

app.listen(8080, function(){
	console.log("IT WORKS!!!");
	dispatch();
});

