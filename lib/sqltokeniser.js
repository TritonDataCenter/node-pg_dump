/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2016 Joyent, Inc.
 */

var mod_stream = require('stream');
var mod_util = require('util');

var mod_assert = require('assert-plus');
var mod_verror = require('verror');

var lib_sqlparser = require('./sqlparser');

var VE = mod_verror.VError;

var INGEST_AGAIN = 1;
var INGEST_NEXT = 2;
var INGEST_ERROR = 3;

function
isdigit(chr)
{
	var cc_0 = '0'.charCodeAt(0);
	var cc_9 = '9'.charCodeAt(0);

	var cc = chr.charCodeAt(0);

	if (cc >= cc_0 && cc <= cc_9)
		return (true);

	return (false);
}

function
isalpha(chr)
{
	var cc_a = 'a'.charCodeAt(0);
	var cc_z = 'z'.charCodeAt(0);
	var cc_A = 'A'.charCodeAt(0);
	var cc_Z = 'Z'.charCodeAt(0);

	var cc = chr.charCodeAt(0);

	if (cc >= cc_a && cc <= cc_z)
		return (true);

	if (cc >= cc_A && cc <= cc_Z)
		return (true);

	return (false);
}

function
isspace(chr)
{
	if (chr === ' ' || chr === '\t' || chr === '\n')
		return (true);

	return (false);
}

function
isoper(chr)
{
	var opers = '+-*/<>=~!@#%^&|`?';

	return (opers.indexOf(chr) !== -1);
}

function
isspecial(chr)
{
	var specials = '$()[],;:*.';

	return (specials.indexOf(chr) !== -1);
}


function
SQLTokeniser()
{
	var self = this;

	mod_stream.Transform.call(this, {
		objectMode: true,
		highWaterMark: 0
	});

	self.sqlt_state = 'REST';
	self.sqlt_state_stack = [];

	self.sqlt_accum = '';
	self.sqlt_dollar_token = null;

	self.sqlt_error = null;
	self.sqlt_line_pos = 0;
	self.sqlt_line = 1;

	self.sqlt_command = [];

	self.sqlt_copy = null;

	self.sqlt_inq = [];
	self.sqlt_attic = [];
}
mod_util.inherits(SQLTokeniser, mod_stream.Transform);

SQLTokeniser.prototype._pop_state = function
_pop_state()
{
	var self = this;

	if (self.sqlt_state_stack.length === 0) {
		throw (new Error('state ' + self.sqlt_state + ' pop fail'));
	}

	var stfr = self.sqlt_state_stack.pop();

	self.sqlt_state = stfr.stfr_state;
	self.sqlt_accum = stfr.stfr_accum;
};

SQLTokeniser.prototype._push_state = function
_push_state(st)
{
	var self = this;

	self.sqlt_state_stack.push({
		stfr_state: self.sqlt_state,
		stfr_accum: self.sqlt_accum
	});

	self.sqlt_state = st;
	self.sqlt_accum = '';
};

SQLTokeniser.prototype._commit = function
_commit(t, v)
{
	var self = this;

	if (t === 'copy_row' || t === 'copy_end') {
		self.push({
			t: t,
			v: v
		});
		return;
	}

	mod_assert.strictEqual(self.sqlt_copy, null, 'sqlt_copy');

	if (t === 'newline' && self.sqlt_command.length === 0) {
		/*
		 * Drop leading newlines from commands.
		 */
		return;
	}

	if (t === 'special' && v === ';') {
		var cmd = self.sqlt_command;
		self.sqlt_command = [];

		/*
		 * Detect a transition to COPY mode.
		 */
		var parsed = lib_sqlparser.parse_command(cmd);

		if (parsed instanceof Error) {
			self.sqlt_error = parsed;
			return;
		}

		if (parsed !== null) {
			mod_assert.string(parsed.command, 'parsed.command');

			switch (parsed.command) {
			case 'copy':
				var delim = parsed.delimiter;

				mod_assert.string(delim, 'delim');
				mod_assert.strictEqual(delim.length,
				    1, 'DELIMITER value must be length 1');
				mod_assert.strictEqual(parsed.format,
				    'text', 'FORMAT must be "text"');

				/*
				 * Store details of the COPY statement
				 * to allow us to correctly process each
				 * row in the ensuing COPY block.
				 */
				self.sqlt_copy = {
					sqcp_table: parsed.table_name,
					sqcp_columns: parsed.column_names,
					sqcp_delim: delim,
					sqcp_null: parsed.null_string,
					sqcp_state: 'REST',
					sqcp_output: {},
					sqcp_output_ncols: 0,
					sqcp_accum: '',
					sqcp_rows: 0
				};
				break;

			default:
				/*
				 * We don't care about anything but COPY
				 * commands at the moment.
				 */
				break;
			}
		}

		self.push({
			t: 'command',
			cmd: cmd
		});

		if (self.sqlt_copy !== null) {
			self.push({
				t: 'copy_begin',
				table_name: self.sqlt_copy.sqcp_table,
				column_names: self.sqlt_copy.sqcp_columns
			});
		}

		return;
	}

	/*
	 * Append this token to the end of the current accumulating command.
	 */
	self.sqlt_command.push({
		t: t,
		v: v
	});
};

SQLTokeniser.prototype._ingest_copy_commit = function
_ingest_copy_commit(value, is_last)
{
	var self = this;

	var sqcp = self.sqlt_copy;

	if (sqcp.sqcp_output_ncols > sqcp.sqcp_columns.length) {
		self.sqlt_error = VE('too many columns on COPY row');
		return (INGEST_ERROR);
	}

	/*
	 * Use the name of the next column for the property on the output
	 * object.
	 */
	sqcp.sqcp_output[sqcp.sqcp_columns[sqcp.sqcp_output_ncols++]] = value;

	if (!is_last) {
		/*
		 * Look for another column.
		 */
		sqcp.sqcp_state = 'NULL_CHECK';
		sqcp.sqcp_accum = '';
		return (INGEST_NEXT);
	}

	if (sqcp.sqcp_output_ncols !== sqcp.sqcp_columns.length) {
		self.sqlt_error = VE('too few columns on COPY row');
		return (INGEST_ERROR);
	}

	/*
	 * This was the last column, and we have the correct number
	 * of columns.  Emit the entire row.
	 */
	self._commit('copy_row', sqcp.sqcp_output);
	sqcp.sqcp_rows++;
	sqcp.sqcp_output = {};
	sqcp.sqcp_output_ncols = 0;

	/*
	 * Look for another row.
	 */
	sqcp.sqcp_state = 'NULL_CHECK';
	sqcp.sqcp_accum = '';
	return (INGEST_NEXT);
};

/*
 * Each line in a COPY block represents a single row in the database table.
 * The COPY block is terminated by a line that consists only of an escaped
 * period.  The complete text format is described in the PostgreSQL
 * documentation for the COPY command.
 *
 * This function implements the state machine for parsing rows in COPY blocks.
 */
SQLTokeniser.prototype._ingest_copy = function
_ingest_copy(chr)
{
	var self = this;

	mod_assert.object(self.sqlt_copy, 'sqlt_copy');
	var sqcp = self.sqlt_copy;

	switch (sqcp.sqcp_state) {
	case 'REST':
		/*
		 * After the COPY FROM STDIN statement terminator, there must
		 * be an additional new line to begin the COPY block.
		 */
		if (chr !== '\n') {
			self.sqlt_error = VE('expected new line ' +
			    'after COPY command');
			return (INGEST_ERROR);
		}
		sqcp.sqcp_state = 'NULL_CHECK';
		return (INGEST_NEXT);

	case 'NULL_CHECK':
		if (sqcp.sqcp_accum.length < sqcp.sqcp_null.length &&
		    chr === sqcp.sqcp_null[sqcp.sqcp_accum.length]) {
			/*
			 * This is the next character in the NULL column
			 * sequence.
			 */
			sqcp.sqcp_accum += chr;
			return (INGEST_NEXT);

		} else if (sqcp.sqcp_accum.length === sqcp.sqcp_null.length) {
			/*
			 * We have accumulated enough characters to allow
			 * detection of a NULL column.
			 */
			mod_assert.strictEqual(sqcp.sqcp_accum,
			    sqcp.sqcp_null, 'accum === null');

			if (chr === sqcp.sqcp_delim) {
				/*
				 * This is a NULL column value, but not the
				 * last column on the line.
				 */
				return (self._ingest_copy_commit(null, false));
			} else if (chr === '\n') {
				/*
				 * A NULL-valued column which is the last
				 * column on this line.
				 */
				return (self._ingest_copy_commit(null, true));
			}
		}

		/*
		 * This is not a NULL-valued column, so regular processing must
		 * be performed.
		 */
		self.sqlt_inq.unshift({
			inq_str: sqcp.sqcp_accum,
			inq_pos: 0
		});
		sqcp.sqcp_accum = '';
		sqcp.sqcp_state = 'COLUMN';
		return (INGEST_AGAIN);

	case 'COLUMN':
		if (chr === sqcp.sqcp_delim) {
			/*
			 * End of a column in the row, but not the last column.
			 */
			return (self._ingest_copy_commit(sqcp.sqcp_accum, false));
		} else if (chr === '\n') {
			/*
			 * End of the last column in the row.
			 */
			return (self._ingest_copy_commit(sqcp.sqcp_accum, true));
		}

		if (chr === '\\') {
			sqcp.sqcp_state = 'COLUMN_ESCAPED';
		} else {
			sqcp.sqcp_accum += chr;
		}
		return (INGEST_NEXT);

	case 'COLUMN_ESCAPED':
		if (chr === '.' && sqcp.sqcp_output_ncols === 0 &&
		    sqcp.sqcp_accum.length === 0) {
			/*
			 * This might be an end-of-data marker.
			 */
			sqcp.sqcp_state = 'MAYBE_EOD';
		} else {
			sqcp.sqcp_state = 'COLUMN';
		}
		sqcp.sqcp_accum += chr;
		return (INGEST_NEXT);

	case 'MAYBE_EOD':
		if (chr !== '\n') {
			/*
			 * False alarm!
			 */
			sqcp.sqcp_state = 'COLUMN';
			return (INGEST_AGAIN);
		}

		self._commit('copy_end', {
			row_count: sqcp.sqcp_rows
		});

		/*
		 * Return to the regular SQL state machine.
		 */
		self.sqlt_copy = null;
		return (INGEST_NEXT);

	default:
		self.sqlt_error = VE('unknown COPY state: "%s"',
		    sqcp.sqcp_state);
		return (INGEST_ERROR);
	}
};

SQLTokeniser.prototype._ingest_sql = function
_ingest_sql(chr)
{
	var self = this;

	switch (self.sqlt_state) {
	case 'REST':
		self.sqlt_accum = '';

		if (isalpha(chr) || chr === '_') {
			self._push_state('NAME');
			return (INGEST_AGAIN);
		}

		if (chr === '\n') {
			self._commit('newline', '\n');
			return (INGEST_NEXT);
		}

		if (isspace(chr)) {
			return (INGEST_NEXT);
		}

		if ('.;,()'.indexOf(chr) !== -1) {
			self._commit('special', chr);
			return (INGEST_NEXT);
		}

		if (chr === '$') {
			self._push_state('DOLLAR1');
			return (INGEST_NEXT);
		}

		if (chr === '-') {
			self._push_state('DASH1');
			return (INGEST_NEXT);
		}

		if (chr === '/') {
			self._push_state('SLASH1');
			return (INGEST_NEXT);
		}

		if (chr === '=' || chr === ':' || chr === '*' || chr === '+') {
			self._push_state('OPERATOR');
			return (INGEST_AGAIN);
		}

		if (isdigit(chr)) {
			self._push_state('NUMBER0');
			return (INGEST_AGAIN);
		}

		if (chr === '\'') {
			self._push_state('STRING');
			return (INGEST_NEXT);
		}

		if (chr === '"') {
			self._push_state('QUOTED_ID');
			return (INGEST_NEXT);
		}

		self.sqlt_error = VE('invalid character "%s"', chr);
		return (INGEST_ERROR);

	case 'DOLLAR1':
		if (chr === '$') {
			self.sqlt_dollar_token = '';
			self.sqlt_state = 'DOLLAR_STRING';
			return (INGEST_NEXT);
		}

		/*
		 * Technically any character is valid in the dollar quoting
		 * tag, but I have not yet seen anything but letters and the
		 * underscore.  If we relax this, we should be careful about
		 * embedded newline characters.
		 */
		if (chr === '_' || isalpha(chr)) {
			self.sqlt_state = 'DOLLAR_STRING';
			self._push_state('DOLLAR_TAG');
			return (INGEST_AGAIN);
		}

		self.sqlt_error = VE('invalid sequence "$%s"', chr);
		return (INGEST_ERROR);

	case 'DOLLAR_TAG':
		if (chr === '$') {
			self.sqlt_dollar_token = self.sqlt_accum;
			self._pop_state();
			return (INGEST_NEXT);
		}

		if (chr === '_' || isalpha(chr)) {
			/*
			 * See comments for DOLLAR1 state.
			 */
			self.sqlt_accum += chr;
			return (INGEST_NEXT);
		}

		self.sqlt_error = VE('invalid sequence "$%s"',
		    self.sqlt_accum + chr);
		return (INGEST_ERROR);

	case 'DOLLAR_STRING':
		if (chr === '$') {
			self._push_state('DOLLAR_STRING_END_TAG');
			return (INGEST_NEXT);
		}

		if (chr === '\n') {
			self.sqlt_error = VE('unterminated string "%s"',
			    self.sqlt_accum);
			return (INGEST_ERROR);
		}

		self.sqlt_accum += chr;
		return (INGEST_NEXT);

	case 'DOLLAR_STRING_END_TAG':
		var t;

		if (chr === '$') {
			if (self.sqlt_accum === self.sqlt_dollar_token) {
				/*
				 * We have reached the end of the string.
				 * The actual string is stored in the
				 * frame above us, which should be a
				 * DOLLAR_STRING frame.
				 */
				self._pop_state();
				self.sqlt_dollar_token = null;
				self._commit('string', self.sqlt_accum);

				/*
				 * Pop the dollar string frame itself.
				 */
				mod_assert.strictEqual(self.sqlt_state,
				    'DOLLAR_STRING');
				self._pop_state();
				return (INGEST_NEXT);
			}

			/*
			 * False alarm.  Flush out the accumulator to the
			 * frame above us.
			 */
			t = self.sqlt_accum;
			self._pop_state();
			self.sqlt_accum += '$' + t + '$';
			return (INGEST_NEXT);
		}

		self.sqlt_accum += chr;

		if (self.sqlt_accum.length < self.sqlt_dollar_token.length) {
			/*
			 * Need to read more tag characters.
			 */
			return (INGEST_NEXT);
		}

		if (self.sqlt_accum === self.sqlt_dollar_token) {
			/*
			 * This is the token!  Now we need a closing dollar
			 * sign.
			 */
			return (INGEST_NEXT);
		}

		/*
		 * False alarm.  Flush out the accumulator to the frame above
		 * us.
		 */
		t = self.sqlt_accum;
		self._pop_state();
		self.sqlt_accum += '$' + t;
		return (INGEST_NEXT);

	case 'QUOTED_ID':
		if (chr === '"') {
			self.sqlt_state = 'QUOTED_ID_QUOTE';
			return (INGEST_NEXT);
		}

		if (chr === '\n') {
			self.sqlt_error = VE('unterminated quoted ' +
			    'identifier "%s"', self.sqlt_accum);
			return (INGEST_ERROR);
		}

		self.sqlt_accum += chr;
		return (INGEST_NEXT);

	case 'QUOTED_ID_QUOTE':
		if (chr === '"') {
			self.sqlt_accum += '"';
			self.sqlt_state = 'QUOTED_ID';
			return (INGEST_NEXT);
		}

		self._commit('quoted_name', self.sqlt_accum);
		self._pop_state();
		return (INGEST_AGAIN);

	case 'STRING':
		if (chr === '\'') {
			self.sqlt_state = 'STRING_QUOTE';
			return (INGEST_NEXT);
		}

		if (chr === '\n') {
			self.sqlt_error = VE('unterminated string "%s"',
			    self.sqlt_accum);
			return (INGEST_ERROR);
		}

		self.sqlt_accum += chr;
		return (INGEST_NEXT);

	case 'STRING_QUOTE':
		if (chr === '\'') {
			self.sqlt_accum += '\'';
			self.sqlt_state = 'STRING';
			return (INGEST_NEXT);
		}

		self._commit('string', self.sqlt_accum);
		self._pop_state();
		return (INGEST_AGAIN);

	case 'OPERATOR':
		if (chr === '=' || chr === ':' || chr === '*' ||
		    chr === '+') {
			self.sqlt_accum += chr;
			return (INGEST_NEXT);
		}

		self._commit('operator', self.sqlt_accum);
		self._pop_state();
		return (INGEST_AGAIN);

	case 'DASH1':
		if (chr === '-') {
			self.sqlt_state = 'DASH_COMMENT';
			return (INGEST_NEXT);
		}

		self.sqlt_error = VE('invalid sequence "-%s"', chr);
		return (INGEST_ERROR);

	case 'DASH_COMMENT':
		if (chr === '\n') {
			self._pop_state();
		}
		return (INGEST_NEXT);

	case 'SLASH1':
		if (chr === '*') {
			self.sqlt_state = 'C_COMMENT';
			return (INGEST_NEXT);
		}

		self.sqlt_error = VE('invalid sequence "/%s"', chr);
		return (INGEST_ERROR);

	case 'C_COMMENT':
		self.sqlt_error = VE('C-style comments not supported');
		return (INGEST_ERROR);

	case 'NAME':
		if (isalpha(chr) || isdigit(chr) || chr === '_' ||
		    chr === '$') {
			self.sqlt_accum += chr;
			return (INGEST_NEXT);
		}

		if (isspace(chr) || isoper(chr) || isspecial(chr)) {
			self._commit('name', self.sqlt_accum);
			self._pop_state();
			return (INGEST_AGAIN);
		}

		self.sqlt_error = VE('invalid character "%s"', chr);
		return (INGEST_ERROR);

	case 'NUMBER0':
		if (isdigit(chr)) {
			self.sqlt_accum += chr;
			return (INGEST_NEXT);
		}

		if (chr === '.') {
			self.sqlt_accum += chr;
			self.sqlt_state = 'NUMBER_DECIMAL';
			return (INGEST_NEXT);
		}

		if (chr === 'e') {
			self.sqlt_accum += chr;
			self.sqlt_state = 'NUMBER_EXPONENT';
			return (INGEST_NEXT);
		}

		self._commit('number', self.sqlt_accum);
		self._pop_state();
		return (INGEST_AGAIN);

	case 'NUMBER_EXPONENT':
		self.sqlt_error = VE('numbers with exponent not supported');
		return (INGEST_ERROR);

	case 'NUMBER_DECIMAL':
		self.sqlt_error = VE('numbers with decimal point not ' +
		    'supported');
		return (INGEST_ERROR);

	default:
		self.sqlt_error = VE('unknown state: "%s"', self.sqlt_state);
		return (INGEST_ERROR);
	}
};

SQLTokeniser.prototype._transform = function
_transform(chunk, _, done)
{
	var self = this;

	if (self.sqlt_error !== null) {
		return;
	}

	/*
	 * First, append the incoming buffer to the end of the input queue.
	 */
	var input = Buffer.isBuffer(chunk) ? chunk.toString('utf8') : chunk;
	self.sqlt_inq.push({
		inq_str: input,
		inq_pos: 0
	});

	/*
	 * Process the input queue one character at a time.  Some parts of the
	 * tokeniser need to look at a variable number of input characters
	 * before making a decision on how to proceed.  These speculative
	 * tokeniser states may push some input data back onto the front of the
	 * input queue, if processing of the same input data by a different
	 * state is required.
	 */
	var again = false;
	for (;;) {
		if (self.sqlt_inq.length < 1) {
			setImmediate(done);
			return;
		}

		var inq = self.sqlt_inq[0];
		if (inq.inq_pos >= inq.inq_str.length) {
			var old = self.sqlt_inq.shift();

			/*
			 * Store a limited number of previous input chunks for
			 * debugging purposes.
			 */
			while (self.sqlt_attic.length >= 4) {
				self.sqlt_attic.shift();
			}
			self.sqlt_attic.push(old);

			continue;
		}

		var chr = inq.inq_str[inq.inq_pos];

		/*
		 * Process this input character.  The COPY block is different
		 * enough from regular SQL that we use a separate parsing state
		 * machine.
		 */
		var action = self.sqlt_copy !== null ?
		    self._ingest_copy(chr) : self._ingest_sql(chr);

		if (!again) {
			/*
			 * For debugging and error reporting purposes,
			 * track the characters we've read on the current
			 * line:
			 */
			if (chr === '\n') {
				self.sqlt_line++;
				self.sqlt_line_pos = 0;
			} else {
				self.sqlt_line_pos++;
			}
		}

		if (self.sqlt_error !== null) {
			setImmediate(done, VE({
				cause: self.sqlt_error,
				info: self._verr_info()
			}, 'invalid SQL stream'));
			return;
		}

		switch (action) {
		case INGEST_NEXT:
			/*
			 * Advance the input cursor.
			 */
			inq.inq_pos++;
			again = false;
			break;

		case INGEST_AGAIN:
			again = true;
			break;

		case INGEST_ERROR:
			/*
			 * We should have stopped already at the check
			 * for "self.sqlt_error" above.
			 */
			throw (VE('unexpected parse error'));

		default:
			throw (VE('invalid ingest action: ' + action));
		}
	}
};

SQLTokeniser.prototype._flush = function
_flush(done)
{
	var self = this;

	/*
	 * If we reach the end of the stream, but are not back in the
	 * REST state, then the input terminated unexpectedly.
	 */
	if (self.sqlt_state !== 'REST' || self.sqlt_accum !== '' ||
	    self.sqlt_state_stack.length > 0 || self.sqlt_copy !== null ||
	    self.sqlt_inq.length > 0) {
		setImmediate(done, VE({
			info: self._verr_info()
		}, 'input stream terminated unexpectedly'));
		return;
	}

	setImmediate(done);
};

SQLTokeniser.prototype._verr_info = function
_verr_info()
{
	var self = this;

	return ({
		line_pos: self.sqlt_line_pos,
		line: self.sqlt_line,
		state: self.sqlt_state,
		accum: self.sqlt_accum,
		dollar_token: self.sqlt_dollar_token,
		copy: self.sqlt_copy,
		inq: self.sqlt_inq,
		inq_attic: self.sqlt_attic
	});
};

module.exports = {
	SQLTokeniser: SQLTokeniser
};
/* vim: set ts=8 sts=8 sw=8 noet: */
