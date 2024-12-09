import { DICOMWeb, utils } from '@ohif/core';
import { sortStudySeries } from '@ohif/core/src/utils/sortStudy';

const { getString, getName, getModalities } = DICOMWeb;

const SQL_STATEMENT_API = "sql/statements/"

function processResults(qidoStudies) {
  if (!qidoStudies || !qidoStudies.length) {
    return [];
  }

  const studies = [];

  qidoStudies.forEach(qidoStudy =>
    studies.push({
      studyInstanceUid: getString(JSON.parse(qidoStudy[0])),
      date: qidoStudy[1] || '', // YYYYMMDD
      time: qidoStudy[2] || '', // HHmmss.SSS (24-hour, minutes, seconds, fractional seconds)
      accession: qidoStudy[3] || '', // short string, probably a number?
      mrn: qidoStudy[4] || '', // medicalRecordNumber
      patientName: utils.formatPN(getName(JSON.parse(qidoStudy[5]))) || '',
      description: getString(JSON.parse(qidoStudy[6])) || '',
      modalities: qidoStudy[7] + qidoStudy[8],
      instances: Number(JSON.parse(qidoStudy[9])) || 1, // number
    })
  );

  return studies;
}

function processSeriesMetadataResults(qidoSeriesMetadata) {
  if (!qidoSeriesMetadata || !qidoSeriesMetadata.length) {
    return [];
  }

  const series = [];

  qidoSeriesMetadata.forEach(currentSerie => {
    var serie = {
      StudyInstanceUid: currentSerie[0],
      SeriesInstanceUid: currentSerie[1],
      instances: JSON.parse(currentSerie[2])
    }

    serie.instances.forEach((instance, index) => {
      instance.StudyInstanceUID = serie.StudyInstanceUid
      instance.SeriesInstanceUID = serie.SeriesInstanceUid
      instance.InstanceNumber = index
      instance.numImageFrames = serie.instances.length
      instance.meta = JSON.parse(instance.meta)
    });

    series.push(serie)
  })

  return series;
}

export function processSeriesResults(qidoSeries) {
  const series = [];

  if (qidoSeries && qidoSeries.length) {
    qidoSeries.forEach(qidoSeries =>
      series.push({
        studyInstanceUid: getString(JSON.parse(qidoSeries[0])),
        seriesInstanceUid: getString(JSON.parse(qidoSeries[1])),
        modality: getString(JSON.parse(qidoSeries[2])),
        seriesNumber: getString(JSON.parse(qidoSeries[3])),
        description: getString(JSON.parse(qidoSeries[4])),
        numSeriesInstances: Number(JSON.parse(qidoSeries[5])),
      })
    );
  }

  sortStudySeries(series);

  return series;
}

async function qidoStudiesSearch(databricksClient, warehouseId, pixelsTable, origParams) {

  var limit = origParams.resultsPerPage ? `LIMIT ${origParams.resultsPerPage}` : "";
  var offset = origParams.offset ? `OFFSET ${origParams.offset}` : "";

  let filters = ["1=1"];
  if ("patientName" in origParams) {
    filters.push(`lower(meta:['00100010'].Value) like lower('%${origParams.patientName}%')`);
  }
  if ("patientId" in origParams && "pageNumber" in origParams) {
    filters.push(`meta:['00100020'].Value[0] like '${origParams.patientId}%'`);
  }
  if ("accessionNumber" in origParams) {
    filters.push(`meta:['00080050'].Value[0] like '${origParams.accessionNumber}%'`);
  }
  if ("studyDescription" in origParams) {
    filters.push(`lower(meta:['00081030'].Value) like lower('%${origParams.studyDescription}%')`);
  }
  if ("modalitiesInStudy" in origParams) {
    filters.push(`(meta:['00080060'].Value[0] in ('${origParams.modalitiesInStudy.join("','")}') OR
                  meta:['00080061'].Value[0] in ('${origParams.modalitiesInStudy.join("','")}'))`);
  }
  if ("startDate" in origParams) {
    filters.push(`'${origParams.startDate}' <= meta:['00080020'].Value[0]`);
  }
  if ("endDate" in origParams) {
    filters.push(`'${origParams.endDate}' >= meta:['00080020'].Value[0]`);
  }

  let body = {
    "warehouse_id": warehouseId,
    "statement": `select
        meta: ['0020000D'] as studyInstanceUid,
        nullif(meta: ['00080020'].Value[0], '') as date,
        nullif(meta: ['00080030'].Value[0], '') as time,
        nullif(meta: ['00080050'].Value[0], '') as accession,
        nullif(meta: ['00100020'].Value[0], '') as mrn,
        first(meta: ['00100010'], true) as patientName,
        first(meta: ['00081030'], true) as description,
        array_join(collect_set(nullif(meta:['00080060'].Value[0], '')), '/') as modalities1,
        array_join(collect_set(nullif(meta:['00080061'].Value[0], '')), '/') as modalities2,
        count(*) as instances
       FROM ${pixelsTable}
       WHERE ${filters.join(" AND ")}
       GROUP BY studyInstanceUid, date, time, accession, mrn
       ${limit}
       ${offset}
       `,
    "wait_timeout": "30s",
    "on_wait_timeout": "CANCEL"
  }

  const result = await databricksClient.post(SQL_STATEMENT_API, body)

  return result.data.result.data_array;
}

async function qidoSeriesSearch(databricksClient, warehouseId, studyInstanceUid, pixelsTable) {

  let body = {
    "warehouse_id": warehouseId,
    "statement": `SELECT *, count(*) as numSeriesInstances from (
      SELECT
        meta:['0020000D'] as studyInstanceUid,
        meta:['0020000E'] as seriesInstanceUid,
        meta:['00080060'] as modality,
        meta:['00200011'] as seriesNumber,
        meta:['0008103E'] as description
        FROM ${pixelsTable}
        WHERE meta:['0020000D'].Value[0] = '${studyInstanceUid}')
      group by studyInstanceUid, seriesInstanceUid, modality, seriesNumber, seriesInstanceUid, description`,
    "wait_timeout": "30s",
    "on_wait_timeout": "CANCEL"
  }

  const result = await databricksClient.post(SQL_STATEMENT_API, body)

  return result.data.result.data_array;
}

async function qidoSeriesMetadataSearch(databricksClient, warehouseId, studyInstanceUid, pixelsTable) {
  var to_return = []
  let body = {
    "warehouse_id": warehouseId,
    "statement": `
      with qico(
        SELECT
              meta:['0020000D'].Value[0] as StudyInstanceUID,
              meta:['0020000E'].Value[0] as SeriesInstanceUID,
              meta:['00080018'].Value[0] as SOPInstanceUID,
              meta:['00080016'].Value[0] as SOPClassUID,
              meta,
              relative_path
        FROM ${pixelsTable}
      )

      select StudyInstanceUID, SeriesInstanceUID, collect_list(
        struct(SOPInstanceUID,
              SOPClassUID,
              meta,
              relative_path)
      ) from qico
        WHERE studyInstanceUid = '${studyInstanceUid}'
        group by studyInstanceUid, seriesInstanceUid`,
      "wait_timeout": "30s",
      "on_wait_timeout": "CANCEL"
  }

  var result = await databricksClient.post(SQL_STATEMENT_API, body);
  to_return = result.data.result.data_array

  while (result.data.result?.next_chunk_internal_link) {
    result = await databricksClient.get(result.data.result.next_chunk_internal_link.split("/api/2.0/")[1])
    to_return = to_return.concat(result.data.data_array)
  }

  return to_return
}

async function persistMetadata(databricksClient, warehouseId, pixelsTable, dataset) {

  let body = {
    "warehouse_id": warehouseId,
    "statement": `INSERT INTO ${pixelsTable}
  (path, modificationTime, length, original_path, relative_path, local_path,
   extension, file_type, path_tags, is_anon, meta, thumbnail)
  VALUES (
   'dbfs:/${dataset.path}',  to_timestamp(unix_timestamp('${dataset.datetime}', 'yyyyMMddHHmmss')), '${dataset.length}', 'dbfs:/${dataset.path}', '${dataset.path}', '/${dataset.path}',
   'dcm', '', array(), 'true', '${dataset.meta}',
   struct( 'ohif_export' AS origin, -1 AS height, -1 AS width, -1 AS nChannels, -1 AS mode, CAST('' AS binary)))`,
    "wait_timeout": "30s",
    "on_wait_timeout": "CANCEL"
  }

  //console.log(body)
  const result = await databricksClient.post(SQL_STATEMENT_API, body)

  return result;
}

/**
 *
 * @param {string} studyInstanceUID - ID of study to return a list of series for
 * @returns {Promise} - Resolves SeriesMetadata[] in study
 */
export function seriesInStudy(dicomWebClient, studyInstanceUID) {
  // Series Description
  // Already included?
  const commaSeparatedFields = ['0008103E', '00080021'].join(',');
  const queryParams = {
    includefield: commaSeparatedFields,
  };

  return dicomWebClient.searchForSeries({ studyInstanceUID, queryParams });
}

/**
 * Produces a QIDO URL given server details and a set of specified search filter
 * items
 *
 * @param filter
 * @param serverSupportsQIDOIncludeField
 * @returns {string} The URL with encoded filter query data
 */
function mapParams(params, options = {}) {
  if (!params) {
    return;
  }
  const commaSeparatedFields = [
    '00081030', // Study Description
    '00080060', // Modality
    // Add more fields here if you want them in the result
  ].join(',');

  const { supportsWildcard } = options;
  const withWildcard = value => {
    return supportsWildcard && value ? `*${value}*` : value;
  };

  const parameters = {
    // Named
    PatientName: withWildcard(params.patientName),
    //PatientID: withWildcard(params.patientId),
    '00100020': withWildcard(params.patientId), // Temporarily to make the tests pass with dicomweb-server.. Apparently it's broken?
    AccessionNumber: withWildcard(params.accessionNumber),
    StudyDescription: withWildcard(params.studyDescription),
    ModalitiesInStudy: params.modalitiesInStudy,
    // Other
    limit: params.limit || 101,
    offset: params.offset || 0,
    fuzzymatching: options.supportsFuzzyMatching === true,
    includefield: commaSeparatedFields, // serverSupportsQIDOIncludeField ? commaSeparatedFields : 'all',
  };

  // build the StudyDate range parameter
  if (params.startDate && params.endDate) {
    parameters.StudyDate = `${params.startDate}-${params.endDate}`;
  } else if (params.startDate) {
    const today = new Date();
    const DD = String(today.getDate()).padStart(2, '0');
    const MM = String(today.getMonth() + 1).padStart(2, '0'); //January is 0!
    const YYYY = today.getFullYear();
    const todayStr = `${YYYY}${MM}${DD}`;

    parameters.StudyDate = `${params.startDate}-${todayStr}`;
  } else if (params.endDate) {
    const oldDateStr = `19700102`;

    parameters.StudyDate = `${oldDateStr}-${params.endDate}`;
  }

  // Build the StudyInstanceUID parameter
  if (params.studyInstanceUid) {
    let studyUids = params.studyInstanceUid;
    studyUids = Array.isArray(studyUids) ? studyUids.join() : studyUids;
    studyUids = studyUids.replace(/[^0-9.]+/g, '\\');
    parameters.StudyInstanceUID = studyUids;
  }

  // Clean query params of undefined values.
  const final = {};
  Object.keys(parameters).forEach(key => {
    if (parameters[key] !== undefined && parameters[key] !== '') {
      final[key] = parameters[key];
    }
  });

  return final;
}

export { mapParams, qidoStudiesSearch, qidoSeriesSearch, qidoSeriesMetadataSearch, processResults, processSeriesMetadataResults, persistMetadata };
