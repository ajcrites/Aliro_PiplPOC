const co = require('co');
const job = require('../services/job.js');

exports.launchPage = function(req, res) {
  res.render('job', {launchPage: true});
};

exports.beginPeopleSearch = (req, res, next) => co(function* () {
  // using a hard coded jobId for POC for now
  var jobId = process.env.JOB_ID;
  const jobDetails = yield job.getJobDetails(jobId);
  const names = yield job.searchCloudSearch(jobDetails);
  const results = yield [
    job.sendNamesToPipl(names),
    job.searchJobTitle(jobDetails),
  ];
  console.log(results);
  const stats = results[0];
  const matches = results[1];
  res.render('job', {jobTitle: jobDetails.title, matches, stats});
});
