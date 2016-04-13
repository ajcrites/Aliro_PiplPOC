"use strict";

// set requires
const mongodb = require('mongodb');
const MongoClient = mongodb.MongoClient;
const ObjectId = mongodb.ObjectId;
const AWS = require('aws-sdk');
const rp = require('request-promise');
const url = require('url');
const lodash = require('lodash');
const co = require('co');
const Promise = require('bluebird');
const delay = require('timeout-as-promise');

const csd = Promise.promisifyAll(new AWS.CloudSearchDomain({endpoint: process.env.CS_SEARCH_DOMAIN, region: process.env.AWS_REGION}));

exports.getJobDetails = jobId => {
  return co(function* () {
    const db = yield MongoClient.connect(process.env.MONGODB_DSN);
    console.log('Successfully connected to Aliro database...');
    const jobDetails = yield db.collection('Job').findOne(
      {_id: ObjectId(jobId)},
      {title: 1, jobFunction: 1, requiredSkills: 1, desiredSkills: 1}
    );

    if (!jobDetails) {
      const err = new Error('No job details found for provided job ID');
      err.status = 404;
      throw err;
    }

    return jobDetails;
  });
};

exports.searchCloudSearch = jobData => {
  return co(function* () {
    const query = '"' +
      [jobData.title, jobData.jobFunction].concat(jobData.desiredSkills, jobData.requiredSkills)
      .map(item => item.replace(/"/g, ""))
      .join('" OR "') + '"';

    const params = {
      queryParser: 'lucene',
      return: 'person_e',
      // size: 700,
      size: 100,
      query,
    };

    let people = {};
    const searchResult = yield csd.searchAsync(params);
    searchResult.hits.hit.forEach(person => {
      if (person && person.fields && person.fields.person_e && person.fields.person_e.length) {
        person.fields.person_e
        .filter(isValidName)
        .forEach(name => people[name] = person.id);
      }
    });

    return Object.keys(people).slice(0, params.size);
  });
};

exports.sendNamesToPipl = names => {
  return co(function* () {
    const db = yield MongoClient.connect(process.env.MONGODB_PIPL_DSN);
    const Person = db.collection('person');
    const stats = {
      single_person: 0,
      possible_persons: 0,
      non_match: 0,
    };

    // pipl limits API calls to 20 per second, so we will split up the array...
    const CHUNK_SIZE = 20;
    const meta = lodash.chunk(names, 20);

    yield meta.reduce((promise, namesArr) => co(function* () {
      yield promise;
      yield namesArr.map(name => co(function* () {
        const body = yield rp({
          url: url.format({
            pathname: 'https://api.pipl.com/search/',
            query: {
              key: process.env.PIPL_API_KEY || 'sample_key',
              raw_name: name,
              country: 'US',
              match_requirements: '(emails and jobs)',
            }
          }),
          json: true,
        });

        if (body.person) {
          const p = Object.assign({full_person: true}, body.person);
          yield Person.insertOne({data: p});
          stats.single_person++;
        }
        else if (body.possible_persons) {
          yield body.possible_persons.map(person => co(function* () {
            const p = Object.assign({full_person: false}, person);
            yield Person.insertOne({data: p});
            stats.possible_persons++;
          }));
        }
        else {
          stats.non_match++;
        }
      }));
      yield delay(1000);
    }).catch(err => console.error(err)), Promise.resolve());

    db.close();

    return stats;
  });
};

exports.searchJobTitle = jobDetails => {
  return co(function* () {
    const db = yield MongoClient.connect(process.env.MONGODB_PIPL_DSN);
    const Person = db.collection('person');

    //TODO remove this
    jobDetails.title = 'Software Engineer';

    let concurrencyCounter = 0;
    let people = yield (yield Person.find({'data.jobs.title': new RegExp(jobDetails.title, 'i')}).toArray()).map(elem => co(function* () {
      if (elem.data && elem.data.full_person) {
        return elem;
      }

      while (concurrencyCounter > 20) {
        yield delay(1000);
      }

      concurrencyCounter++;
      const body = yield rp({
        url: url.format({
          pathname: 'https://api.pipl.com/search/',
          query: {
            key: process.env.PIPL_API_KEY || 'sample_key',
            search_pointer: elem.data['@search_pointer']
          },
        }),
        json: true,
      });
      concurrencyCounter--;

      if (!body.person) {
        console.error('No body found for pointer search', body);
      }
      else {
        const p = Object.assign({full_person: true}, body.person);
        return yield Person.findOneAndUpdate(
          {_id: elem._id},
          {$set: {data: p}},
          {returnOriginal: false}
        );
      }
    }));

    return people;
  }).catch(err => console.error(err));
};

function isValidName(name) {
  return name.match(/^\s*([a-z]{2,})\s+([a-z]{2,})\s*$/gi) != null
  || name.match(/^\s*([a-z]{2,})\s+([a-z]{2,})\s+([a-z]{2,})\s*$/gi) != null
  || name.match(/^\s*([a-z]{2,})\s+([a-z])\s+([a-z]{2,})\s*$/gi) != null
  || name.match(/^\s*([a-z]{2,})\s+([a-z])-([a-z]{2,})\s*$/gi) != null;
}
