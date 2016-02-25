import React, { Component, PropTypes } from 'react';
import { isLoaded as stateLoaded, load as loadState } from 'redux/modules/clusterstate';
import { isLoaded as endpointsLoaded, load as loadEndpoints } from 'redux/modules/endpoints';
import { asyncConnect } from 'redux-async-connect';
import { connect } from 'react-redux';

@asyncConnect([{
  deferred: true,
  promise: ({store: {dispatch, getState}}) => {
    const promises = [];
    if (!stateLoaded(getState())) {
      promises.push(dispatch(loadState()));
    }
    if (!endpointsLoaded(getState())) {
      promises.push(dispatch(loadEndpoints()));
    }
    return Promise.all(promises);
  }
}])
@connect(
  state => ({
    clusterState: state.clusterState.data,
    endpoints: state.endpoints.data,
  })
)
export default class Home extends Component {
  static propTypes = {
    clusterState: PropTypes.object,
    endpoints: PropTypes.object.isRequired,
  };
  render() {
    const styles = require('./Home.scss');

    const clusterClasses = ['fa'];
    const clusterStyle = {'fontSize': '30vh'};
    let clusterText;
    if (!this.props.clusterState) {
      clusterClasses.push('fa-meh-o');
      clusterStyle.color = 'yellow';
      clusterText = '?';
    } else if (this.props.clusterState.health) {
      clusterClasses.push('fa-smile-o');
      clusterStyle.color = 'lime';
      clusterText = 'Cluster is healthy';
    } else {
      clusterStyle.push('fa-frown-o');
      clusterStyle.color = 'red';
      clusterText = 'Cluster is unhealthy';
    }

    let dbservers = [];
    let coordinators = [];

    if (this.props.endpoints.dbservers) {
      dbservers = this.props.endpoints.dbservers.map(dbserver => {
        return (<li key={dbserver}>{dbserver}</li>);
      });
    }
    let clusterAccess;
    if (this.props.endpoints.coordinators) {
      coordinators = this.props.endpoints.coordinators.map(coordinator => {
        return (<li key={coordinator}><a href={coordinator}>{coordinator}</a></li>);
      });
      clusterAccess = (<div className={styles.action}>Access your cluster: <a href={this.props.endpoints.coordinators[0]}>{this.props.endpoints.coordinators[0]}</a></div>);
    }

    return (
      <div className={styles['flex-container']}>
        <div className={styles['flex-item']}>
          <i className={clusterClasses.join(' ')} style={clusterStyle}/>
          <h3>{clusterText}</h3>
        </div>
        <div className={styles['flex-item']}>
          <div className={styles.dialog}>
            <div className={styles.header}>
              Cluster Setup
            </div>
            <div className={styles.content}>
              <div>
                <div className={styles.subheadline}>Coordinators</div>
                <ul>
                  {coordinators}
                </ul>
                <div className={styles.subheadline}>DBServers</div>
                <ul>
                  {dbservers}
                </ul>
              </div>
              {clusterAccess}
            </div>
          </div>
        </div>
      </div>
    );
  }
}
