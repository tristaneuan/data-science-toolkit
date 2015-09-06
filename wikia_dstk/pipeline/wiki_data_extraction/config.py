config = {
    "region": "us-west-2",
    "price": "0.300",
    "ami": "ami-7ffdef4f",  # dstk 2015-09-06b
    "key": "data-extraction",
    "sec": "sshable",
    "type": "m2.4xlarge",
    "tag": "wiki_data_extraction",
    "threshold": 50,
    "git_ref": "master",
    "max_size": 5,  # 5
    "services": ",".join([
        "TopEntitiesService",
        "EntityDocumentCountsService",
        "TopHeadsService",
        "WpTopEntitiesService",
        "WpEntityDocumentCountsService"
    ])
}
