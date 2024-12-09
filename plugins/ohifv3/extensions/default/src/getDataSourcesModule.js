// TODO: Pull in IWebClientApi from @ohif/core
// TODO: Use constructor to create an instance of IWebClientApi
// TODO: Use existing DICOMWeb configuration (previously, appConfig, to configure instance)

import { createDicomWebApi } from './DicomWebDataSource/index.js';
import { createDicomJSONApi } from './DicomJSONDataSource/index.js';
import { createDicomLocalApi } from './DicomLocalDataSource/index.js';
import { createDatabricksPixelsDicom } from './DatabricksPixelsDicom/index.js';

/**
 *
 */
function getDataSourcesModule() {
  return [
    {
      name: 'dicomweb',
      type: 'webApi',
      createDataSource: createDicomWebApi,
    },
    {
      name: 'dicomjson',
      type: 'jsonApi',
      createDataSource: createDicomJSONApi,
    },
    {
      name: 'dicomlocal',
      type: 'localApi',
      createDataSource: createDicomLocalApi,
    },
    {
      name: 'databricksPixelsDicom',
      type: 'webApi',
      createDataSource: createDatabricksPixelsDicom,
    },
  ];
}

export default getDataSourcesModule;
