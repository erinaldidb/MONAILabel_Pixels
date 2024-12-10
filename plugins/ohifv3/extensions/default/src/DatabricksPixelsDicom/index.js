import { DicomMetadataStore, IWebApiDataSource, utils } from '@ohif/core';
import OHIF from '@ohif/core';
import dcmjs from 'dcmjs';

import axios from "axios";
import sha1 from "crypto-js/sha1";

import {
  mapParams,
  qidoSeriesSearch,
  qidoSeriesMetadataSearch,
  qidoStudiesSearch,
  processResults,
  processSeriesResults,
  processSeriesMetadataResults,
  persistMetadata
} from './utils.js';

const metadataProvider = OHIF.classes.MetadataProvider;

const { DicomMetaDictionary, DicomDict } = dcmjs.data;
const { naturalizeDataset, denaturalizeDataset } = DicomMetaDictionary;

function createDatabricksPixelsDicom(dcmConfig, servicesManager) {

  let dicomConfig,
    databricksClient,
    warehouseId;

  console.info("createDatabricksPixelsDicom")

  const implementation = {
    initialize: async ({ params, query }) => {

      console.info("createDatabricksPixelsDicom - Initialize")

      dicomConfig = JSON.parse(JSON.stringify(dcmConfig));

      if (!dicomConfig.token || !dicomConfig.serverHostname || !dicomConfig.httpPath || !dicomConfig.pixelsTable) {
        throw new Error("Cannot find Server Hostname, HTTP Path, or " +
          "personal access token. " +
          "Check the environment variables DATABRICKS_SERVER_HOSTNAME, " +
          "DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN and pixelsTable.");
      }

      const connectOptions = {
        token: dicomConfig.token,
        host: dicomConfig.serverHostname,
        path: dicomConfig.httpPath
      };

      databricksClient = axios.create({
        baseURL: connectOptions.host + "/api/2.0/",
      });

      databricksClient.defaults.headers.common['Authorization'] = `Bearer ${connectOptions.token}`
      const userAuthenticationService = servicesManager.services.userAuthenticationService;
      userAuthenticationService.getAuthorizationHeader = () => {
        return { 'Authorization': `Bearer ${connectOptions.token}` }
      }

      warehouseId = dicomConfig.httpPath.split("/")[4]

    },
    query: {
      studies: {
        mapParams: mapParams.bind(),
        search: async function (origParams) {
          console.info("createDatabricksPixelsDicom - query studies")

          const results = await qidoStudiesSearch(databricksClient, warehouseId, dicomConfig.pixelsTable, origParams);

          return processResults(results);
        },
        processResults: processResults.bind(),
      },
      series: {
        search: async studyInstanceUID => {
          console.info("createDatabricksPixelsDicom - query series", studyInstanceUID)

          const series = await qidoSeriesSearch(databricksClient, warehouseId, studyInstanceUID, dicomConfig.pixelsTable);
          const result = processSeriesResults(series);
          return result
        },
      },
      instances: {
        search: () => {
          console.warn(' QUERY instances SEARCH not implemented');
        },
      },
    },
    retrieve: {
      directURL: params => {
        const { instance, tag, defaultType } = params;

        console.log("retrieve direct url")
        const value = instance[tag];
        if (value instanceof Array && value[0] instanceof ArrayBuffer) {
          return URL.createObjectURL(
            new Blob([value[0]], {
              type: defaultType,
            })
          );
        }
      },
      bulkDataURI: async ({ StudyInstanceUID, BulkDataURI }) => {
        console.warn(' Retrieve bulkDataURI not implemented');
      },
      series: {
        metadata: async ({
          StudyInstanceUID,
          madeInClient = false
        } = {}) => {
          if (!StudyInstanceUID) {
            throw new Error('Unable to query for SeriesMetadata without StudyInstanceUID');
          }

          const study = await qidoSeriesMetadataSearch(databricksClient, warehouseId, StudyInstanceUID, dicomConfig.pixelsTable);
          const result = processSeriesMetadataResults(study);

          const seriesSummaryMetadata = {};
          const instancesPerSeries = {};


          result.forEach(aSeries => {

            aSeries.instances.forEach((instance, index) => {

              const naturalizedInstancesMetadata = naturalizeDataset(instance.meta);

              naturalizedInstancesMetadata.url = "dicomweb:" + databricksClient.defaults.baseURL + "fs/files/" + instance.relative_path

              const {
                url: imageId,
                StudyInstanceUID,
                SeriesInstanceUID,
                SOPInstanceUID,
              } = naturalizedInstancesMetadata;

              naturalizedInstancesMetadata.imageId = imageId;
              naturalizedInstancesMetadata.wadoUri = databricksClient.defaults.baseURL + "fs/files/" + instance.relative_path

              naturalizedInstancesMetadata.volumeRoot = instance.relative_path.split("/").slice(0, 4).join("/")

              if (!seriesSummaryMetadata[naturalizedInstancesMetadata.SeriesInstanceUID]) {
                seriesSummaryMetadata[naturalizedInstancesMetadata.SeriesInstanceUID] = {
                  StudyInstanceUID: naturalizedInstancesMetadata.StudyInstanceUID,
                  StudyDescription: naturalizedInstancesMetadata.studyDescription,
                  SeriesInstanceUID: naturalizedInstancesMetadata.SeriesInstanceUID,
                  SeriesDescription: naturalizedInstancesMetadata.seriesDescription,
                  SOPInstanceUID: naturalizedInstancesMetadata.SOPInstanceUID,
                  SeriesNumber: naturalizedInstancesMetadata.seriesNumber,
                  SeriesTime: naturalizedInstancesMetadata.seriesTime,
                  SOPClassUID: naturalizedInstancesMetadata.sopClassUID,
                  ProtocolName: naturalizedInstancesMetadata.protocolName,
                  Modality: naturalizedInstancesMetadata.modality,
                };
              }

              if (!instancesPerSeries[naturalizedInstancesMetadata.SeriesInstanceUID]) {
                instancesPerSeries[naturalizedInstancesMetadata.SeriesInstanceUID] = [];
              }

              // Add imageId specific mapping to this data as the URL isn't necessarily WADO-URI.
              metadataProvider.addImageIdToUIDs(imageId, {
                StudyInstanceUID,
                SeriesInstanceUID,
                SOPInstanceUID,
                frameIndex: 1,
              });

              instancesPerSeries[naturalizedInstancesMetadata.SeriesInstanceUID].push(naturalizedInstancesMetadata);

            });

            // grab all the series metadata
            const seriesMetadata = Object.values(seriesSummaryMetadata);
            DicomMetadataStore.addSeriesMetadata(seriesMetadata, madeInClient);

            Object.keys(instancesPerSeries).forEach(SeriesInstanceUID =>
              DicomMetadataStore.addInstances(instancesPerSeries[SeriesInstanceUID], madeInClient)
            );

          });
        }
      },
    },
    store: {
      dicom: async naturalizedReport => {

        naturalizedReport.SeriesNumber = "9999" //hack for monailabel ordering fix
        const reportBlob = dcmjs.data.datasetToBlob(naturalizedReport);
        var  instance = {}
        if (naturalizedReport.ConceptNameCodeSequence?.CodeValue == '126000'){
        //this is a measurement
          instance = DicomMetadataStore.getInstance(
            naturalizedReport.StudyInstanceUID,
            naturalizedReport.CurrentRequestedProcedureEvidenceSequence[0].ReferencedSeriesSequence.SeriesInstanceUID,
            naturalizedReport.CurrentRequestedProcedureEvidenceSequence[0].ReferencedSeriesSequence.ReferencedSOPSequence.ReferencedSOPInstanceUID
          );
        }else {
          //this is a segmentation
          instance = DicomMetadataStore.getInstance(
            naturalizedReport.StudyInstanceUID,
            naturalizedReport.ReferencedSeriesSequence.SeriesInstanceUID,
            naturalizedReport.ReferencedSeriesSequence.ReferencedInstanceSequence[0].ReferencedSOPInstanceUID
          );
        }

        const volumePath = instance.volumeRoot
        const filePath = volumePath + "/ohif/exports/" +
          naturalizedReport.StudyInstanceUID + "/" +
          naturalizedReport.SeriesInstanceUID + "/" +
          naturalizedReport.SOPInstanceUID + ".dcm"

        const response = await databricksClient.put("fs/files/" + filePath,
          reportBlob, {
          headers: { 'Content-Type': 'application/octet-stream' },
          maxBodyLength: Infinity,
          maxContentLength: Infinity,
          responseType: 'json'
        })

        if (response.status == 204) {
          console.log("File saved correclty in volume path", naturalizedReport.filePath)
        }

        var meta = denaturalizeDataset(naturalizedReport)
        delete meta['7FE00010']
        delete meta['60003000']
        meta['fileSize'] = reportBlob.size
        meta['hash'] = sha1(reportBlob).toString()

        naturalizedReport.meta = meta
        console.log(naturalizedReport)

        const result = await persistMetadata(
          databricksClient,
          warehouseId,
          dicomConfig.pixelsTable,
          {
            path: filePath,
            meta: JSON.stringify(naturalizedReport.meta),
            datetime: "" + naturalizedReport.ContentDate + naturalizedReport.ContentTime,
            length: reportBlob.size
          }
        )
      }
    },
    getImageIdsForDisplaySet(displaySet) {
      const images = displaySet.images;
      const imageIds = [];

      if (!images) {
        return imageIds;
      }

      displaySet.images.forEach(instance => {
        const NumberOfFrames = instance.NumberOfFrames;
        if (NumberOfFrames > 1) {
          // in multiframe we start at frame 1
          for (let i = 1; i <= NumberOfFrames; i++) {
            const imageId = this.getImageIdsForInstance({
              instance,
              frame: i,
            });
            imageIds.push(imageId);
          }
        } else {
          const imageId = this.getImageIdsForInstance({ instance });
          imageIds.push(imageId);
        }
      });

      return imageIds;
    },
    getImageIdsForInstance({ instance, frame }) {
      const { StudyInstanceUID, SeriesInstanceUID, SOPInstanceUID } = instance;
      const storedInstance = DicomMetadataStore.getInstance(
        StudyInstanceUID,
        SeriesInstanceUID,
        SOPInstanceUID
      );

      let imageId = storedInstance.url;

      if (frame !== undefined) {
        imageId += `&frame=${frame}`;
      }

      return imageId;
    },
    deleteStudyMetadataPromise() {
      console.log('deleteStudyMetadataPromise not implemented');
    },
    getStudyInstanceUIDs: ({ params, query }) => {
      console.log("getStudyInstanceUIDs", params, query)
      const { StudyInstanceUIDs: paramsStudyInstanceUIDs } = params;
      const queryStudyInstanceUIDs = utils.splitComma(query.getAll('StudyInstanceUIDs'));

      const StudyInstanceUIDs =
        (queryStudyInstanceUIDs.length && queryStudyInstanceUIDs) || paramsStudyInstanceUIDs;
      const StudyInstanceUIDsAsArray =
        StudyInstanceUIDs && Array.isArray(StudyInstanceUIDs)
          ? StudyInstanceUIDs
          : [StudyInstanceUIDs];

      return StudyInstanceUIDsAsArray;
    },
  };
  return IWebApiDataSource.create(implementation);
}

export { createDatabricksPixelsDicom };
