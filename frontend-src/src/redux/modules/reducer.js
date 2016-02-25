import { combineReducers } from 'redux';
import { routeReducer } from 'react-router-redux';
import {reducer as reduxAsyncConnect} from 'redux-async-connect';

import clusterState from './clusterstate';
import endpoints from './endpoints';

export default combineReducers({
  routing: routeReducer,
  reduxAsyncConnect,
  clusterState,
  endpoints,
});
