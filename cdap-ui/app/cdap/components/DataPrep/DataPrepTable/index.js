/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import React, { Component } from 'react';
import DataPrepStore from 'components/DataPrep/store';
import shortid from 'shortid';
import ee from 'event-emitter';
import ColumnActionsDropdown from 'components/DataPrep/ColumnActionsDropdown';
require('./DataPrepTable.scss');
import {execute} from 'components/DataPrep/store/DataPrepActionCreator';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import TextboxOnValium from 'components/TextboxOnValium';

export default class DataPrepTable extends Component {
  constructor(props) {
    super(props);

    let storeState = DataPrepStore.getState().dataprep;

    this.state = {
      headers: storeState.headers.map(header => ({name: header, edit: false})),
      data: storeState.data,
      loading: !storeState.initialized,
      directivesLength: storeState.directives.length,
      workspaceId: storeState.workspaceId
    };

    this.eventEmitter = ee(ee);
    this.openUploadData = this.openUploadData.bind(this);
    this.openCreateWorkspaceModal = this.openCreateWorkspaceModal.bind(this);
    this.switchToEditColumnName = this.switchToEditColumnName.bind(this);
    this.sub = DataPrepStore.subscribe(() => {
      let state = DataPrepStore.getState().dataprep;
      this.setState({
        data: state.data,
        headers: state.headers.map(header => ({name: header, edit: false})),
        loading: !state.initialized,
        directivesLength: state.directives.length,
        workspaceId: state.workspaceId
      });
    });
  }

  componentWillUnmount() {
    this.sub();
  }

  openCreateWorkspaceModal() {
    this.eventEmitter.emit('DATAPREP_CREATE_WORKSPACE');
  }

  openUploadData() {
    this.eventEmitter.emit('DATAPREP_OPEN_UPLOAD');
  }

  switchToEditColumnName(head) {
    let newHeaders = this.state.headers.map(header => {
      if (header.name === head.name) {
        return Object.assign({}, header, {
          edit: !header.edit
        });
      }
      return header;
    });
    this.setState({
      headers: newHeaders
    });
  }

  handleSaveEditedColumnName(index, changedValue, noChange) {
    let headers = this.state.headers;
    let matchedHeader = headers[index];
    if (!noChange) {
      this.applyDirective(`rename ${matchedHeader.name} ${changedValue}`);
      matchedHeader.name = changedValue;
    }
    matchedHeader.edit = false;
    this.setState({
      headers: [
        ...headers.slice(0, index),
        matchedHeader,
        ...headers.slice(index + 1)
      ]
    });
  }
  applyDirective(directive) {
    execute([directive])
      .subscribe(
        () => {},
        (err) => {
          DataPrepStore.dispatch({
            type: DataPrepActions.setError,
            payload: {
              message: err.message || err.response.message
            }
          });
        }
      );
  }
  render() {
    if (this.state.loading) {
      return (
        <div className="dataprep-table empty">
          <h4 className="text-xs-center">
            <span className="fa fa-spin fa-spinner" />
          </h4>
        </div>
      );
    }

    let headers = this.state.headers;
    let data = this.state.data;

    if (!this.state.workspaceId) {
      return (
        <div className="dataprep-table empty">
          <div>
            <h5 className="text-xs-center">Please create a workspace to wrangle data</h5>
            <div className="button-container text-xs-center">
              <button
                className="btn btn-primary"
                onClick={this.openCreateWorkspaceModal}
              >
                Create
              </button>
            </div>
          </div>
        </div>
      );
    }

    if (data.length === 0 || headers.length === 0) {
      return (
        <div className="dataprep-table empty">
          {
            this.state.directivesLength === 0 ? (
              <div>
                <h5 className="text-xs-center">Start by uploading data to this workspace</h5>
                <div className="button-container text-xs-center">
                  <button
                    className="btn btn-primary"
                    onClick={this.openUploadData}
                  >
                    Upload
                  </button>
                </div>
              </div>
            ) : null
          }
        </div>
      );
    }

    return (
      <div className="dataprep-table" id="dataprep-table-id">
        <table className="table table-bordered table-striped">
          <thead className="thead-inverse">
            {
              headers.map((head, index) => {
                return (
                  <th
                    id={`column-${head.name}`}
                    key={head.name}
                  >
                    <div
                      className="clearfix column-wrapper"
                    >
                      {
                        !head.edit ?
                          <span
                            className="header-text float-xs-left"
                            onClick={this.switchToEditColumnName.bind(this, head)}
                          >
                            {head.name}
                          </span>
                        :
                          <TextboxOnValium
                            onChange={this.handleSaveEditedColumnName.bind(this, index)}
                            value={head.name}
                          />
                      }
                      <span className="float-xs-right directives-dropdown-button">
                        <ColumnActionsDropdown
                          column={head.name}
                        />
                      </span>
                    </div>
                  </th>
                );
              })
            }
          </thead>
          <tbody>
            {
              data.map((row) => {
                return (
                  <tr key={shortid.generate()}>
                    {headers.map((head) => {
                      return <td key={shortid.generate()}><div>{row[head.name]}</div></td>;
                    })}
                  </tr>
                );
              })
            }
          </tbody>
        </table>
      </div>
    );
  }
}
