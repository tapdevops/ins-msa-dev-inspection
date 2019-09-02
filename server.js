/*
|--------------------------------------------------------------------------
| Global APP Init
|--------------------------------------------------------------------------
*/
	global._directory_base = __dirname;
	global.config = {};
		  config.app = require( './config/app.js' );
		  config.database = require( './config/database.js' )[config.app.env];

	// Models
	const ViewInspection = require( _directory_base + '/app/v1.0/Http/Models/ViewInspectionModel.js' );

/*
|--------------------------------------------------------------------------
| APP Setup
|--------------------------------------------------------------------------
*/
	// Node Modules
	const body_parser = require( 'body-parser' );
	const express = require( 'express' );
	const mongoose = require( 'mongoose' );
	const timeout = require( 'connect-timeout' );

	// Primary Variable
	const app = express();
	var data_source_request = {
		"auth":false,
		"finding":false
	}
	var kafka = require("kafka-node"),
	Producer = kafka.Producer,
	Consumer = kafka.Consumer,
	client = new kafka.KafkaClient({kafkaHost : "149.129.252.13:9092"}),
	producer = new Producer(client),    
	consumer = new Consumer(
        client,
        [
            { topic: 'kafkaRequestData', partition: 0 },{ topic: 'kafkaDataCollectionProgress', partition: 0 },{ topic: 'kafkaResponse', partition: 0 }
        ],
        {
            autoCommit: false
        }
    );
	consumer.on('message', function (message) {
		json_message = JSON.parse(message.value);
		if(message.topic=="kafkaRequestData"){
			//ada yang request data ke microservices
			let reqDataObj;
			let responseData = false;
			if(json_message.msa_name=="inspection"){
				 if( json_message.agg  ){
					const matchJSON = JSON.parse( json_message.agg );
					console.log( "matchJSON", matchJSON );
					const set = ViewInspection.aggregate( [	
						matchJSON
					] )
					reqDataObj = {
						"msa_name":json_message.msa_name,
						"model_name":json_message.model_name,
						"requester":json_message.requester,
						"request_id":json_message.request_id,
						"data": set
					}
					responseData = true;
				 }
				 
			}
			if( responseData ){
				let payloads = [
					{ topic: "kafkaResponseData", messages: JSON.stringify( reqDataObj ), partition: 0 }
				];
				producer.send( payloads, function( err, data ){
					console.log( "Send data to kafka", data );
				} );
			}
		}
	});

	// let count = 0;
	// let reqObj = {
	// 	"data_source":[{
	// 		"msa_name":"auth",
	// 		"model_name":"UserAuth"
	// 	},{
	// 		"msa_name":"inspection",
	// 		"model_name":"InspectionModel"
	// 	}],
	// 	"query":"SELECT DISTINCT a.INSPECTION_CODE,a.CREATOR,b.NAME CREATOR_NAME FROM inspection_InspectionModel a LEFT JOIN auth_UserAuth b ON a.CREATOR=b.CREATOR",
	// 	"requester":"inspection",
	// 	"request_id":2
	// }

	// producer.on("ready", function() {
	// 	//console.log(JSON.stringify(reqObj));
	// 	//setInterval(function() {
	// 		//minta data
	// 		payloads = [
	// 			{ topic: "kafkaRequest", messages: JSON.stringify(reqObj), partition: 0 }
	// 		];
	// 		producer.send( payloads, function( err, data ) {
	// 			console.log( "Send to kafka request data" );
	// 		});
	// 	//}, 2000);
	// });

	// producer.on("error", function(err) {
	// 	console.log( err );
	// });

/*
|--------------------------------------------------------------------------
| APP Init
|--------------------------------------------------------------------------
*/
	// Parse request of content-type - application/x-www-form-urlencoded
	app.use( body_parser.urlencoded( { extended: false } ) );

	// Parse request of content-type - application/json
	app.use( body_parser.json() );

	// Timeout Handling
	app.use( timeout( 3600000 ) );
	app.use( halt_on_timeout );

	function halt_on_timeout( req, res, next ){
		if ( !req.timedout ) {
			 next();
		}
		else {
			return res.json( {
				status: false,
				message: "Connection Timeout",
				data: {}
			} )
		}
	}

	// Setup Database
	mongoose.Promise = global.Promise;
	mongoose.connect( config.database.url, {
		useNewUrlParser: true,
		ssl: config.database.ssl
	} ).then( () => {
		console.log( "Database :" );
		console.log( "\tStatus \t\t: Connected" );
		console.log( "\tMongoDB URL \t: " + config.database.url + " (" + config.app.env + ")" );
	} ).catch( err => {
		console.log( "Database :" );
		console.log( "\tDatabase Status : Not Connected" );
		console.log( "\tMongoDB URL \t: " + config.database.url + " (" + config.app.env + ")" );
	} );

	// Server Running Message
	var server = app.listen( parseInt( config.app.port[config.app.env] ), () => {
		console.log( "Server :" );
		console.log( "\tStatus \t\t: OK" );
		console.log( "\tService \t: " + config.app.name + " (" + config.app.env + ")" );
		console.log( "\tPort \t\t: " + config.app.port[config.app.env] );
	} );
	server.timeout = 100000;

	// Routing
	require( './routes/api.js' )( app );
	module.exports = app;