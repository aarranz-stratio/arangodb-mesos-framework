import React, { Component, PropTypes } from 'react';

export default class App extends Component {
  static propTypes = {
    children: PropTypes.object.isRequired,
  };

  static contextTypes = {
    store: PropTypes.object.isRequired
  };

  render() {
    const styles = require('./App.scss');

    const floating = {float: 'right'};
    const logo = require('./logo_arangodb_white.png');

    return (
      <div className={styles.app}>
        <nav className="navbar">
          <div className="resizecontainer">
            <div className="navlogo">
              <a className="logo" href="#"><img src={logo}/></a>
            </div>
            <div id="progressPlaceholderIcon"></div>
              <div className="statmenu" id="statisticBar">
              </div>
              <div className="usermenu" id="userBar" style={floating}>
              </div>
              <div className="notificationmenu" id="notificationBar" style={floating}>
              </div>
              <div className="navmenu" id="navigationBar">
              </div>
          </div>
        </nav>

        <div className="centralRow resizecontainer">
          <div id="content" className="centralContent">
            {this.props.children}
          </div>
        </div>

        <footer className="footer">
          <div className="resizecontainer" id="footerBar">
          </div>
        </footer>
      </div>
    );
  }
}
