default_config = {
    "queue": "data_events",
    "region": "us-west-2",
    "price": "0.300",
    "ami": "ami-0bfdef3b",  # dstk 2015-09-06a
    "key": "data-extraction",
    "sec": "sshable",
    "type": "m2.4xlarge",
    "tag": "data_extraction",
    "threshold": 50,
    "max_size": 5,
    "git_ref": "master",
    "services": ",".join([
        "AllNounPhrasesService",
        "AllVerbPhrasesService",
        "HeadsService",
        "CoreferenceCountsService",
        "EntityCountsService",
        "DocumentSentimentService",
        "DocumentEntitySentimentService",
        "WpDocumentEntitySentimentService"
    ])
}
