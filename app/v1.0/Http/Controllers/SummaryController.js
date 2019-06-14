/*
 |--------------------------------------------------------------------------
 | App Setup
 |--------------------------------------------------------------------------
 |
 | Untuk menghandle models, libraries, helper, node modules, dan lain-lain
 |
 */
 	// Models
	const InspectionDModel = require( _directory_base + '/app/v1.0/Http/Models/InspectionDModel.js' );
	const InspectionHModel = require( _directory_base + '/app/v1.0/Http/Models/InspectionHModel.js' );
	const InspectionTrackingModel = require( _directory_base + '/app/v1.0/Http/Models/InspectionTrackingModel.js' );

	// Node Module
	const MomentTimezone = require( 'moment-timezone' );

/*
 |--------------------------------------------------------------------------
 | Versi 1.0
 |--------------------------------------------------------------------------
 */

 	exports.compute_distance = ( lat1, lon1, lat2, lon2) => {
		var R = 6371; // Lingkar Bumi (KM)
		var dLat = ( lat2 - lat1 ) * Math.PI / 180;
		var dLon = ( lon2 - lon1 ) * Math.PI / 180;
		var a = Math.sin( dLat/2 ) * Math.sin( dLat/2 ) +
			Math.cos( lat1 * Math.PI / 180 ) * Math.cos( lat2 * Math.PI / 180 ) *
			Math.sin( dLon / 2 ) * Math.sin(dLon/2);
		var c = 2 * Math.atan2( Math.sqrt( a ), Math.sqrt( 1 - a ) );
		var d = R * c;

		return Math.round( d * 1000 );
	}
 	/** 
 	  * Total Inspeksi Jarak
	  * --------------------------------------------------------------------
	*/
	exports.total_jarak = async ( req, res ) => {
		var query = await InspectionTrackingModel.aggregate( [
			{
				"$sort": {
					"_id": 1
				}
			},
			{
				"$match": {
					"INSERT_USER": req.auth.USER_AUTH_CODE
				}
			},
			{
				"$project": {
					"_id": 0,
					"TRACK_INSPECTION_CODE": 1,
					"BLOCK_INSPECTION_CODE": 1,
					"DATE_TRACK": 1,
					"LAT_TRACK": 1,
					"LONG_TRACK": 1
				}
			}
		] );
		var total_meter_distance = 0;
		var total_km_distance = 0;

		if ( query.length > 0 ) {
			for ( var i = 0; i <= ( query.length - 1 ); i++ ) {
				if ( i < ( query.length - 1 ) ) {
					var j = i + 1;
					var track_1 = query[i];
					var track_2 = query[j];
					var compute_distance = exports.compute_distance( track_1.LAT_TRACK, track_1.LONG_TRACK, track_2.LAT_TRACK, track_2.LONG_TRACK );
					total_meter_distance += compute_distance;
				}
			}
		}
		
		res.status( 200 ).json( {
			status: true,
			message: "Success!",
			data: {
				distance_meter: total_meter_distance,
				distance_km: ( total_meter_distance / 1000 ),
				duration: 0
			}
		} );
	};

 	/** 
 	  * Total Inspeksi
	  * --------------------------------------------------------------------
	*/
 	exports.total_inspeksi = async ( req, res ) => {
 		var date = new Date();
 			date.setDate( date.getDate() - 1 );
 		var max_inspection_date = parseInt( MomentTimezone( date ).tz( "Asia/Jakarta" ).format( "YYYYMMDD" ) + '235959' );
 		var inspection_test = await InspectionHModel.aggregate( [
			{
				"$match": {
					"INSERT_USER": req.auth.USER_AUTH_CODE
				}
			},
			{
				"$group": {
					"_id": {
						"WERKS": "$WERKS",
						"AFD_CODE":"$AFD_CODE",
						"BLOCK_CODE": "$BLOCK_CODE"
					},
					"count": {
						"$sum": 1
					}
				}
			}
		] );
		var results = {
			total_inspeksi_baris: 0,
			total_block: 0
		};

		if ( inspection_test.length > 0 ) {
			results.total_block = inspection_test.length;
			
			inspection_test.forEach( function( dt ) {
				results.total_inspeksi_baris = results.total_inspeksi_baris + dt.count;
			} );
		}

 		res.status( 200 ).json( {
 			status: true,
 			message: "Success!",
 			data: results
 		} );
 	}