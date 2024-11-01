#!/bin/bash

# Copyright (c) MONAI Consortium
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

curr_dir="$(pwd)"
my_dir="$(dirname "$(readlink -f "$0")")"

echo "Installing requirements..."
sh $my_dir/requirements.sh

install_dir=${1:-$my_dir/../../monailabel/endpoints/static/ohif}

echo "Current Dir: ${curr_dir}"
echo "My Dir: ${my_dir}"
echo "Installing OHIF at: ${install_dir}"

cd ${my_dir}
rm -rf Viewers
git clone https://github.com/OHIF/Viewers.git
cd Viewers
git checkout d8ef36ed24466988586e19b855d2bbb86f8c657a

#cp -r ../extensions/monai-label extensions/
#cp -r ../modes/monai-label modes/monai-label
cd extensions
ln -s ../../extensions/monai-label monai-label
cd ..

cd modes
ln -s ../../modes/monai-label monai-label
cd ..

git apply ../extensions.patch

cp ../config/monai_label.js platform/app/public/config/monai_label.js

#copy Databricks Pixels integration
echo "Installing Databricks Integration"
mkdir ./extensions/default/src/DatabricksPixelsDicom/
cp ../extensions/default/src/DatabricksPixelsDicom/index.js ./extensions/default/src/DatabricksPixelsDicom/index.js
cp ../extensions/default/src/DatabricksPixelsDicom/utils.js ./extensions/default/src/DatabricksPixelsDicom/utils.js
cp ../extensions/default/src/getDataSourcesModule.js ./extensions/default/src/getDataSourcesModule.js

yarn config set workspaces-experimental true
yarn install
<<<<<<< HEAD
yarn run cli list

APP_CONFIG=config/monai_label.js PUBLIC_URL=/ohif/ QUICK_BUILD=true yarn run build
=======
APP_CONFIG=config/monai_label.js PUBLIC_URL=./ QUICK_BUILD=true yarn run build
>>>>>>> b723bb8 (added pixels solution accelerator integration)

rm -rf ${install_dir}
cp -r platform/app/dist/ ${install_dir}
echo "Copied OHIF to ${install_dir}"
<<<<<<< HEAD

cd ..
rm -rf Viewers
find .  -type d -name "node_modules" -exec rm -rf "{}" +
=======
#rm -rf ../Viewers
>>>>>>> b723bb8 (added pixels solution accelerator integration)

cd ${curr_dir}