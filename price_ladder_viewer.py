#!/usr/bin/python3

# Copyright 2019 Peter Dymkar Brandt All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================

"""Price Ladder Viewer is a tool for visualizing historical tick data from
electronically traded financial instruments.

It pulls data in tickdata.com format from a MySQL database, and uses QT GUI
components to animate a price ladder view of the market for a given time window.

Example:
    $ python price_ladder_viewer.py --config_file config.yaml
"""

import argparse
import math
import sys
import time
import yaml

import numpy as np
import pandas as pd
import sqlalchemy as sql
from PySide2 import QtCore, QtGui, QtWidgets

class MarketData():
    """Contains functionality for pulling market data from the database, and for
    constructing and updating the price ladder.
    """
    def __init__(self, config):
        """Set up initial price ladder state and pull market data.

        Args:
            config: Determines behavior for connecting to database, formatting
            price ladder strings, etc.
        """
        self._config = config
        self._price_ladder = None
        self._price_ladder_df = pd.DataFrame(
            '', range(self._config['row_count']), range(5))
        self._quotes_row = 0
        self._trades_row = 0

        # Connect to database, get dataframes, and close connection.
        connect_string = 'mysql://{}:{}@{}/{}'.format(
            self._config['db_user'], self._config['db_password'],
            self._config['db_host'], self._config['db_name'])
        engine = sql.create_engine(connect_string)
        connection = engine.connect()
        quotes_query = ('SELECT time_value, bid_price, bid_size, '
                        'ask_price, ask_size '
                        "FROM {}.{} WHERE symbol='{}' AND date_value='{}' AND "
                        "time_value>='{}' AND time_value<='{}' "
                        'ORDER BY time_value ASC').format(
                            self._config['db_name'],
                            self._config['db_table_quotes'],
                            self._config['symbol'],
                            self._config['date'],
                            self._config['start_time'],
                            self._config['end_time'])
        self._quotes_df = pd.read_sql_query(quotes_query, connection)
        trades_query = ('SELECT time_value, price, volume FROM {}.{} WHERE '
                        "symbol='{}' AND date_value='{}' AND "
                        "time_value>='{}' AND time_value<='{}' "
                        'ORDER BY time_value ASC').format(
                            self._config['db_name'],
                            self._config['db_table_trades'],
                            self._config['symbol'],
                            self._config['date'],
                            self._config['start_time'],
                            self._config['end_time'])
        self._trades_df = pd.read_sql_query(trades_query, connection)
        connection.close()

        # Print counts of rows pulled from database.
        print('Database quote count:{}, trade count:{}'.format(
            len(self._quotes_df.index), len(self._trades_df.index)))

    def get_next_price_ladder_df(self):
        """Step forward chronologically and update the price ladder.

        Returns:
            The updated price ladder dataframe.
        """
        # Check that the end of the market data hasn't been reached.
        if (self._quotes_row > len(self._quotes_df.index) - 1 and \
            self._trades_row > len(self._trades_df.index) - 1):
            return None

        # Call update helper functions depending on whether a quote or trade is
        # next chronologically.
        if self._quotes_row > len(self._quotes_df.index) - 1:
            self._update_trade()
        elif self._trades_row > len(self._trades_df.index) - 1:
            self._update_quote()
        elif (self._quotes_df.loc[self._quotes_row, 'time_value'] <
              self._trades_df.loc[self._trades_row, 'time_value']):
            self._update_quote()
        else:
            self._update_trade()

        return self._price_ladder_df

    def get_price_ladder_time_elapsed(self):
        """Determine time delay until the next price ladder update.

        Returns:
            Time delay in seconds.
        """
        if (self._quotes_row > len(self._quotes_df.index) - 1 and \
            self._trades_row > len(self._trades_df.index) - 1):
            return 0.0

        min_time_value = min(self._quotes_df.loc[0, 'time_value'],
                             self._trades_df.loc[0, 'time_value'])
        current_time_value = min(
            self._quotes_df.loc[self._quotes_row, 'time_value'],
            self._trades_df.loc[self._trades_row, 'time_value'])
        return current_time_value - min_time_value

    def _update_quote(self):
        """Helper function to update price ladder based on next quote row."""
        # If this is the first quote or a price is outside current price ladder,
        # reset the price ladder.
        if (self._quotes_row == 0 or (
                self._quotes_df.loc[self._quotes_row, 'ask_price'] > \
                self._price_ladder[0] + .5 * self._config['tick_size']) or (
                    self._quotes_df.loc[self._quotes_row, 'bid_price'] < \
                    self._price_ladder[-1] - .5 * self._config['tick_size'])):
            max_price = (self._quotes_df.loc[self._quotes_row, 'ask_price'] +
                         self._config['tick_size'] * np.floor(
                             (self._config['row_count'] - 1) / 2))
            self._price_ladder = np.linspace(
                max_price,
                max_price - (
                    self._config['row_count'] - 1) * self._config['tick_size'],
                self._config['row_count'])
            self._price_ladder_df.iloc[:, [0, 1, 3, 4]] = ''
            self._price_ladder_df.iloc[:, 2] = [self._config[
                'price_format'].format(x) for x in self._price_ladder]

        # Populate price ladder dataframe and update table cells.
        for i in range(self._config['row_count']):
            if math.isclose(self._price_ladder[i],
                            self._quotes_df.loc[self._quotes_row, 'ask_price']):
                self._price_ladder_df.iloc[i, 3] = str(
                    self._quotes_df.loc[self._quotes_row, 'ask_size'])
            else:
                self._price_ladder_df.iloc[i, 3] = ''
            if math.isclose(self._price_ladder[i],
                            self._quotes_df.loc[self._quotes_row, 'bid_price']):
                self._price_ladder_df.iloc[i, 1] = str(
                    self._quotes_df.loc[self._quotes_row, 'bid_size'])
            else:
                self._price_ladder_df.iloc[i, 1] = ''

        # Print this quote row and update counter.
        print(self._quotes_df.iloc[self._quotes_row, ].values)
        self._quotes_row += 1

    def _update_trade(self):
        """Helper function to update price ladder based on next trade row."""
        # Populate price ladder dataframe. Assign trade to a side assuming there
        # isn't both a bid and ask at the same price. Aggregate consecutive
        # trades at the same price and populate cumulative volume.
        if self._quotes_row > 0:
            for i in range(self._config['row_count']):
                if math.isclose(self._price_ladder[i],
                                self._trades_df.loc[self._trades_row, 'price']):
                    volume = self._trades_df.loc[self._trades_row, 'volume']
                    if self._price_ladder_df.iloc[i, 1]:
                        if self._price_ladder_df.iloc[i, 0]:
                            volume += int(self._price_ladder_df.iloc[i, 0])

                        self._price_ladder_df.iloc[i, 0] = str(volume)
                        self._price_ladder_df.iloc[i, 4] = ''
                    elif self._price_ladder_df.iloc[i, 3]:
                        if self._price_ladder_df.iloc[i, 4]:
                            volume += int(self._price_ladder_df.iloc[i, 4])

                        self._price_ladder_df.iloc[i, 0] = ''
                        self._price_ladder_df.iloc[i, 4] = str(volume)
                else:
                    self._price_ladder_df.iloc[i, [0, 4]] = ''

        # Print this trade row and update counter.
        print(self._trades_df.iloc[self._trades_row, ].values)
        self._trades_row += 1

class Window(QtWidgets.QWidget):
    """Manages the visual style and lifecycle of the GUI."""
    def __init__(self, config, market_data, *args):
        """Creates the main GUI window and table, and starts the worker thread.

        Args:
            config: Determines visual style for the GUI, etc.
            market_data: An instance of MarketData from which the price ladder
                is obtained.
        """
        QtWidgets.QWidget.__init__(self, *args)

        self._config = config
        self.setGeometry(
            0, 0, self._config['window_width'], self._config['window_height'])
        self.setWindowTitle(self._config['symbol'])
        self._table_model = TableModel(self, self._config, market_data)

        table_view = QtWidgets.QTableView()
        table_view.setModel(self._table_model)
        table_view.setFont(QtGui.QFont(self._config['font_name'],
                                       self._config['font_size']))
        table_view.horizontalHeader().setVisible(False)
        table_view.verticalHeader().setVisible(False)
        table_view.setShowGrid(False)

        layout = QtWidgets.QVBoxLayout(self)
        layout.addWidget(table_view)
        self.setLayout(layout)

        # Start the worker thread. This is needed because we can't sleep and
        # update the price ladder on the main GUI thread.
        self._thread = Worker(self._table_model.update_logic)
        self._thread.update_signal.connect(self.on_update_signal)
        self._thread.start()

    def closeEvent(self, event):
        """Properly shut down the worker thread before destructing to prevent a
        crash.

        Args:
            event: QT event triggered when the window is closed.
        """
        self._thread.terminate()
        self._thread.wait()
        event.accept()

    @QtCore.Slot(object)
    def on_update_signal(self):
        """Trigger a refresh of the entire table when the worker thread signals
        that data has changed.
        """
        self._table_model.dataChanged.emit(
            self._table_model.index(0, 0),
            self._table_model.index(self._config['row_count'] - 1, 5),
            [QtCore.Qt.DisplayRole])

class TableModel(QtCore.QAbstractTableModel):
    """Puts price ladders obtained from MarketData into a QT table.
    """
    def __init__(self, parent, config, market_data, *args):
        """Populates private member variables and initializes table.

        Args:
            parent: Window object containing this table.
            config: Determines visual style for the GUI, etc.
            market_data: An instance of MarketData from which the price ladder
                is obtained.
        """
        QtCore.QAbstractTableModel.__init__(self, parent, *args)
        self._config = config
        self._market_data = market_data
        self._start_timestamp = pd.Timestamp.now()
        self._price_ladder_df = None
        self._mutex = QtCore.QMutex()

    def rowCount(self, parent):  # pylint: disable=unused-argument
        """Get the count of rows.

        Returns:
            Count of rows.
        """
        return self._config['row_count']

    def columnCount(self, parent):  # pylint: disable=unused-argument
        """Get the count of columns.

        Returns:
            Count of columns.
        """
        return 5

    def data(self, index, role):
        """Get value for a specific table cell and QT role.
        Args:
            index: QT object indicating table cell.
            role: QT role.

        Returns:
            Value to render in the table cell depending on QT role.
        """
        if not index.isValid():
            return None

        # Set cell background color from config.
        if role == QtCore.Qt.BackgroundRole:
            column_color = self._config['column_colors'][index.column()]
            return QtGui.QColor(column_color[0], column_color[1],
                                column_color[2])

        # Set cell string contents from price ladder.
        if role == QtCore.Qt.DisplayRole:
            # pylint: disable=unused-variable
            locker = QtCore.QMutexLocker(self._mutex)
            if self._price_ladder_df is not None:
                return self._price_ladder_df.iloc[index.row(), index.column()]

        return None

    def update_logic(self):
        """Function repeatedly called by the worker thread to update the table.

        Returns:
            Bool indicating whether table data needs to be updated.
        """
        # Calculate time delay until next price ladder, taking into account
        # speed of animation. Sleep to ensure animation pace is correct.
        price_ladder_time_elapsed = (
            self._market_data.get_price_ladder_time_elapsed())
        time_elapsed = pd.Timestamp.now() - self._start_timestamp
        # pylint: disable=too-many-function-args
        delay = (price_ladder_time_elapsed - self._config['speed'] * \
                 time_elapsed / self._config['speed']) / \
                 np.timedelta64(1, 's')
        if delay > 0.0:
            time.sleep(delay)

        # Safely update the price ladder and return.
        # pylint: disable=unused-variable
        locker = QtCore.QMutexLocker(self._mutex)
        self._price_ladder_df = self._market_data.get_next_price_ladder_df()
        return self._price_ladder_df is not None

class Worker(QtCore.QThread):
    """Simple worker thread which drives price ladder animation."""
    update_signal = QtCore.Signal(object)

    def __init__(self, update_func):
        """Save function to be called by thread.

        Args:
            update_func: Function that takes no arguments and returns a bool.
        """
        QtCore.QThread.__init__(self)
        self._update_func = update_func

    def run(self):
        """Call function and emit update signal until function returns False."""
        while self._update_func():
            self.update_signal.emit(None)

def main():
    """Main function for this script."""

    # Parse command line args.
    parser = argparse.ArgumentParser()
    parser.add_argument('--config_file', metavar='FILE', help='config YAML',
                        default='config.yaml')
    args = parser.parse_args()

    # Load config from YAML file.
    with open(args.config_file, 'r') as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as error:
            print(error)
            sys.exit()

    # Run the application.
    market_data = MarketData(config)
    application = QtWidgets.QApplication(sys.argv)
    window = Window(config, market_data)
    window.show()
    sys.exit(application.exec_())

if __name__ == '__main__':
    main()
