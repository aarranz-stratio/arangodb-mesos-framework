
const LOAD = 'endpoints/LOAD';
const LOAD_SUCCESS = 'endpoints/LOAD_SUCCESS';
const LOAD_FAIL = 'endpoints/LOAD_FAIL';

const initialState = {
  loaded: false,
  data: {},
};

export default function reducer(state = initialState, action = {}) {
  switch (action.type) {
    case LOAD:
      return {
        ...state,
        loading: true
      };
    case LOAD_SUCCESS:
      return {
        ...state,
        loading: false,
        loaded: true,
        data: action.result,
        error: null
      };
    case LOAD_FAIL:
      return {
        ...state,
        loading: false,
        loaded: false,
        data: null,
        error: action.error
      };
    default:
      return state;
  }
}

export function isLoaded(globalState) {
  return globalState.endpoints && globalState.endpoints.loaded;
}

export function load() {
  return {
    types: [LOAD, LOAD_SUCCESS, LOAD_FAIL],
    promise: (client) => client.get('/endpoints.json') // params not used, just shown as demonstration
  };
}
