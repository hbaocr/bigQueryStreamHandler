const { Client } = require('elasticsearch');
const { get, isNaN, reverse, uniq, spread, union } = require('lodash');
const Json = require('elasticsearch/src/lib/serializers/json');
const BigJSON = require('json-bigint');
const moment = require('moment');
const BlueBird = require('bluebird');
const sizeof = require('object-sizeof');
const events = require('events');
const colors = require('colors');
const MAX_HITS = 10000; // move to const;
HEAL_DATA_INDEX_PREFIX = 'idlogiq-mhealth_';

const hwIds = [281474971140096, 281474971140098, 281474971140097, 239936293543022, 215871989547295];
const vitalSignField = 'blood_glucose';

const { Readable,Writable } = require("stream");






/* Returns the approximate memory usage, in bytes, of the specified object. The
 * parameter is:
 *
 * object - the object whose size should be determined
 */
function getMemorySizeOBJ(object){

  // initialise the list of objects and size
  var objects = [object];
  var size    = 0;

  // loop over the objects
  for (var index = 0; index < objects.length; index ++){

    // determine the type of the object
    switch (typeof objects[index]){

      // the object is a boolean
      case 'boolean': size += 4; break;

      // the object is a number
      case 'number': size += 8; break;

      // the object is a string
      case 'string': size += 2 * objects[index].length; break;

      // the object is a generic object
      case 'object':

        // if the object is not an array, add the sizes of the keys
        if (Object.prototype.toString.call(objects[index]) != '[object Array]'){
          for (var key in objects[index]) size += 2 * key.length;
        }

        // loop over the keys
        for (var key in objects[index]){

          // determine whether the value has already been processed
          var processed = false;
          for (var search = 0; search < objects.length; search ++){
            if (objects[search] === objects[index][key]){
              processed = true;
              break;
            }
          }

          // queue the value to be processed if appropriate
          if (!processed) objects.push(objects[index][key]);

        }

    }

  }

  // return the calculated size
  return size;

}

function formatByteSize(bytes) {
  if(bytes < 1024) return bytes + " bytes";
  else if(bytes < 1048576) return(bytes / 1024).toFixed(3) + " KiB";
  else if(bytes < 1073741824) return(bytes / 1048576).toFixed(3) + " MiB";
  else return(bytes / 1073741824).toFixed(3) + " GiB";
};


const VITAL_SIGN_ES_MAPPING = {
  OXYGEN_SATURATION: 'oxygen_saturation',
  HEART_RATE: 'heart_rate',
  BLOOD_PRESSURE: 'blood_pressure',
  BLOOD_GLUCOSE: 'blood_glucose',
  AMBIENT_TEMPERATURE: 'ambient_temperature',
  BODY_TEMPERATURE: 'body_temperature',
  SPO2: 'oxygen_saturation',
  ECG: 'electro_cardio_gram',
  BODY_WEIGHT: 'body_weight',
  STEP_COUNT: 'step_count',
  BODY_MASS_INDEX: 'body_mass_index',
  BODY_FAT_PERCENTAGE: 'body_fat_percentage',
  CUSTOM_MUSCLE_MASS: 'custom_muscle_mass',
  CUSTOM_WATER_PERCENTAGE: 'custom_water_percentage',
  CUSTOM_VISCERAL_FAT: 'custom_visceral_fat',
  CUSTOM_BONE_MASS: 'custom_bone_mass',
  CUSTOM_BASAL_METABOLIC_RATE: 'custom_basal_metabolic_rate',
  CUSTOM_PROTEIN_PERCENTAGE: 'custom_protein_percentage',
  CUSTOM_SUBCUTANEOUS_FAT_PERCENTAGE: 'custom_subcutaneous_fat_percentage',
  CUSTOM_FAT_LEVEL: 'custom_fat_level',
  CUSTOM_BODY_TYPE: 'custom_body_type',
  CUSTOM_SUGGEST_WEIGHT: 'custom_suggest_weight',
};

const formatECGData = (data) => ({
  startTime: get(data, 'effective_time_frame.time_interval.start_date_time'),
  endTime: get(data, 'effective_time_frame.time_interval.end_date_time'),
  electro_cardio_gram: get(data, 'electro_cardio_gram', []).map((d) => ({
    dateTime: get(d, 'effective_time_frame.date_time'),
    value: get(d, 'value'),
  })),
});

const defaultFormatter = (vitalSign, data) => ({
  startTime: get(data, 'effective_time_frame.time_interval.start_date_time'),
  endTime: get(data, 'effective_time_frame.time_interval.end_date_time'),
  dateTime: get(data, 'effective_time_frame.date_time'),
  value: get(data, `${vitalSign}.value`),
});

const bloodPressureFormatter = (data) => ({
  dateTime: get(data, 'effective_time_frame.date_time'),
  systolic: get(data, 'systolic_blood_pressure.value'),
  diastolic: get(data, 'diastolic_blood_pressure.value'),
});

const spo2Formatter = (data) => ({
  dateTime: get(data, 'effective_time_frame.date_time'),
  spo2: get(data, 'oxygen_saturation.value'),
  pi: get(data, 'pi.value'),
  pleth: get(data, 'pleth', []).map((d) => ({
    dateTime: get(d, 'effective_time_frame.date_time'),
    value: get(d, 'value'),
  })),
});

const formatter = (vitalSign, data) => {
  if (!data) return {};
  let responseData = {};
  switch (vitalSign) {
    case VITAL_SIGN_ES_MAPPING.ECG:
      responseData = formatECGData(data);
      break;
    case VITAL_SIGN_ES_MAPPING.BLOOD_PRESSURE:
      responseData = bloodPressureFormatter(data);
      break;
    case VITAL_SIGN_ES_MAPPING.SPO2:
      responseData = spo2Formatter(data);
      break;
    default:
      responseData = defaultFormatter(vitalSign, data);
      break;
  }
  return responseData;
};

const getSortDateTimeByVitalSign = (vitalSign) => {
  switch (vitalSign) {
    case VITAL_SIGN_ES_MAPPING.HEART_RATE:
    case VITAL_SIGN_ES_MAPPING.STEP_COUNT:
    case VITAL_SIGN_ES_MAPPING.BLOOD_GLUCOSE:
      return [
        {
          [`${vitalSign}.effective_time_frame.time_interval.end_date_time`]: {
            order: 'desc',
            unmapped_type: 'long',
          },
        },
      ];
    default:
      return [
        {
          [`${vitalSign}.effective_time_frame.date_time`]: {
            order: 'desc',
            unmapped_type: 'long',
          },
        },
      ];
  }
};

Json.prototype.deserialize = (str) => {
  if (typeof str === 'string') {
    try {
      return BigJSON.parse(str);
    } catch (e) {}
  }
};

const clientConfig = {
  host: 'localhost',
  port: 9200,
  auth: `:`,
};

const elasticsearch = new Client({
  host: clientConfig,
  requestTimeout: 60000,
});

const enumerateDaysBetweenDates = (startTime, endTime, format = 'YYYYMMDD') => {
  const startDate = moment.utc(startTime);
  const endDate = moment.utc(endTime);
  const dates = [endDate.format(format)];

  while (startDate.isBefore(endDate)) {
    dates.push(startDate.format(format));
    startDate.add(1, 'days');
  }
  return dates;
};

const getIndexNameInTimeRange = (startTime, endTime) => {
  if (isNaN(startTime) || isNaN(endTime)) {
    return `${HEAL_DATA_INDEX_PREFIX}*`;
  }
  const days = enumerateDaysBetweenDates(startTime, endTime) || [];
  const indexName = days.map((day) => `${HEAL_DATA_INDEX_PREFIX}${day}`).join(',');
  console.log(`joined index ${indexName} from time: ${startTime}, to time: ${endTime}`);
  return indexName;
};

// const searchAfterV2 = async (client, params) => {
//   const emitter = new events.EventEmitter();
//   let result = [];
//   emitter.on('close', (res) => {
//     result = res;
//   });
//   emitter.on('search-after', (response) => {

//   })
// };
let memSize=0;
let chSz = 0;




const searchAfter = async (client, params) => {
  let response = await client.search(params);
  const allResponse = [];
  // eslint-disable-next-line no-constant-condition
  while (true) {
    let sourceHits = (response && response.hits && response.hits.hits) || [];
   chSz=getMemorySizeOBJ(sourceHits[0])*sourceHits.length;
   //chSz =getMemorySizeOBJ(sourceHits);
    memSize=memSize+ chSz;
    formatByteSize
    allResponse.push(...sourceHits);
    try {
      global.gc();
    } catch (error) {
      console.log(error);
    }
  
    console.log(
      `ChunkSiz: ${formatByteSize(chSz)} Memsize: ${formatByteSize(memSize)}  The script uses approximately ${
        Math.round((process.memoryUsage().heapUsed / 1024 / 1024) * 100) / 100
      } MB with length ${allResponse.length}`.yellow
    );


    if (sourceHits.length === 0) {
      break;
    }
    const [lastDoc] = sourceHits.slice(-1);
    const searchAfterKey = (lastDoc && lastDoc.sort) || null;
    if (!searchAfterKey) {
      break;
    }

    console.log(`search_after_key: ${JSON.stringify(searchAfterKey)}`);

    const { body } = params;

    // eslint-disable-next-line no-await-in-loop
    const t0 = Date.now();
    response = await client.search({
      ...params,
      body: {
        ...body,
        search_after: searchAfterKey,
      },
    });
    const t1 = Date.now();
    const total = moment.duration(t1 - t0).asMilliseconds();
    const transfer_time = total - response.took;
    console.log(
      `[getRawHealthDataV3] search after for size ${params.size} with es took ${
        response.took
      } and api took ${transfer_time} ms and json size ${Buffer.byteLength(JSON.stringify(response)) / 1024 / 1024}`
    );
    delete sourceHits;
  }
  return allResponse;
};







const run = async (startTime, endTime) => {
   const t0 = Date.now();
  // if (!vitalSignField) {
  //   return {};
  // }
  const indexes = getIndexNameInTimeRange(startTime, endTime);
  const sort = getSortDateTimeByVitalSign(vitalSignField);
  const filterPath = `took,hits.hits.sort, hits.hits._source.${vitalSignField}, hits.hits._source.measured_date, hits.hits._source.hwId`;

  // // const extraVitalSign = vitalSignField === VITAL_SIGN_ES_MAPPING.ECG ? VITAL_SIGN_ES_MAPPING.HEART_RATE : '';
 
  // // filterPath = extraVitalSign ? `${filterPath}, hits.hits._source.${extraVitalSign}` : filterPath;

  const must = [];

  // at least match this one
  must.push({
    exists: {
      field: vitalSignField,
    },
  });

  must.push({
    bool: {
      filter: {
        term: {
          userId: '6680467792895934464',
        },
      },
    },
  });

  must.push({
    bool: {
      filter: {
        terms: {
          hwId: hwIds,
        },
      },
    },
  });

  must.push({
    bool: {
      filter: {
        range: {
          measured_date: {
            gte: startTime,
            lt: endTime + 1,
          },
        },
      },
    },
  });

  const body = {
    sort,
    query: {
      bool: {
        must,
      },
    },
  };

  // console.log(`getRawHealthData request body: ${JSON.stringify(body)}, indexName: ${indexes}`);

  // const response = await elasticsearch.search({
  //   size: 0,
  //   index: indexes,
  //   ignoreUnavailable: true,
  //   body: {
  //     query: {
  //       bool: {
  //         must,
  //       },
  //     },
  //     size: 0,
  //     aggs: {
  //       time: {
  //         date_histogram: {
  //           min_doc_count: 1,
  //           field: 'measured_date',
  //           fixed_interval: '5m',
  //         },
  //       },
  //     },
  //   },
  // });
  // const intervals = [startTime];
  // const bucketTime = get(response, 'aggregations.time.buckets');
  // let counter = 0;
  // const MAX_HITS = 10000; // move to const;
  // bucketTime.forEach((bucket, index) => {
  //   const { doc_count } = bucket;
  //   counter += doc_count;
  //   if (counter >= MAX_HITS) {
  //     const ts = bucketTime[Math.max(index - 1, index)].key;
  //     intervals.push(ts > startTime ? ts : startTime);
  //     counter = doc_count;
  //   }
  // });
  // const chunkIntervals = reverse(
  //   uniq(intervals).map((item, index) => [item, index === intervals.length - 1 ? endTime + 1 : intervals[index + 1]])
  // );
  // let result = await BlueBird.map(
  //   chunkIntervals,
  //   async (time) => {
  //     body.query.bool.must[body.query.bool.must.length - 1].bool.filter.range.measured_date.gte = time[0];
  //     body.query.bool.must[body.query.bool.must.length - 1].bool.filter.range.measured_date.lt = time[1];
  //     console.log(
  //       `The script uses approximately ${Math.round((process.memoryUsage().heapUsed / 1024 / 1024) * 100) / 100} MB`
  //     );
  //     return searchAfter(elasticsearch, {
  //       size: MAX_HITS,
  //       index: indexes,
  //       filterPath,
  //       ignoreUnavailable: true,
  //       body,
  //     });
  //   },
  //   { concurrency: 16 }
  // );
  // result = spread(union)(result);
  let result = await searchAfter(elasticsearch, {
    size: MAX_HITS,
    index: indexes,
    filterPath,
    ignoreUnavailable: true,
    body,
  });

  // console.log(JSON.stringify(result));
  console.log(result.length);

  const t1 = Date.now();
  console.log(`[getRawHealthDataV3] search after took ${moment.duration(t1 - t0).asSeconds()} seconds`);

  // result = await BlueBird.reduce(
  //   result,
  //   (total, hit) => {
  //     const hwid = get(hit, '_source.hwId');
  //     const data = formatter(vitalSignField, get(hit, `_source.${vitalSignField}`));
  //     const dateTime = get(data, 'dateTime') || get(data, 'endTime');
  //     if (vitalSignField === VITAL_SIGN_ES_MAPPING.ECG)
  //       total[hwid].push(
  //         get(data, 'electro_cardio_gram', []).reduce((ecg, d) => [...ecg, get(d, 'value'), get(d, 'dateTime')], [])
  //       );
  //     else if (vitalSignField === VITAL_SIGN_ES_MAPPING.BLOOD_PRESSURE)
  //       total[hwid].push([get(data, 'systolic'), get(data, 'diastolic'), dateTime]);
  //     else if (vitalSignField === VITAL_SIGN_ES_MAPPING.SPO2) total[hwid].push([get(data, 'spo2'), dateTime]);
  //     else total[hwid].push([get(data, 'value'), dateTime]);
  //     return total;
  //   },
  //   hwIds.reduce((total, h) => ({ ...total, [h]: [] }), {})
  // );

  const t2 = Date.now();
  console.log(`[getRawHealthData] process result took ${moment.duration(t2 - t1).asSeconds()} seconds`);
};

run(1606755600000, 1608051599999);



const inStream = new Readable({
  read(size) {

    if(this.rea){

    }


  }
});

inStream.currentCharCode = 65;