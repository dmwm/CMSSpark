// MongoDB index creation. Indexing is the power source of the performance of MongoDB. There can be only 1 text index for 1 collection.

// switch to rucio db
use rucio;

// datasets indexes
db.datasets.createIndex( { "_id": 1, "Dataset": 1 } );
db.datasets.createIndex( { "Dataset": 1 } );
db.datasets.createIndex( { "LastAccessMs": 1 } );
db.datasets.createIndex( { "Max": 1 } );
db.datasets.createIndex( { "Min": 1 } );
db.datasets.createIndex( { "Avg": 1 } );
db.datasets.createIndex( { "Sum": 1 } );
db.datasets.createIndex( { "RealSize": 1 } );
db.datasets.createIndex( { "TotalFileCnt": 1 } );
db.datasets.createIndex( { "RseType": "text", "Dataset": "text", "RSEs": "text"} );

// detailed_datasets indexes
db.detailed_datasets.createIndex( { "_id": 1, "Type": 1 } );
db.detailed_datasets.createIndex( { "_id": 1, "Dataset": 1 } );
db.detailed_datasets.createIndex( { "_id": 1, "Dataset": 1, "RSE":1 } );
db.detailed_datasets.createIndex( {"Dataset": 1} );
db.detailed_datasets.createIndex( { "Dataset": 1, "RSE":1 } );
db.detailed_datasets.createIndex( { "Type": "text", "Dataset": "text", "RSE": "text", "Tier": "text", "C": "text", "RseKind": "text","ProdAccts": "text"} );
