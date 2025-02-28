export class LuffyDBClient {
  private baseUrl: string;
  private verbose: boolean;
  private authKey: string;

  constructor(url = 'http://localhost:3100', verbose = true, authKey = 'none') {
    this.baseUrl = url;
    this.verbose = verbose;
    this.authKey = authKey;
  }

  // Log messages if verbose mode is enabled
  private log(message: string): void {
    if (this.verbose) {
      console.log(`[LuffyDBClient] ${message}`);
    }
  }

  // Log errors
  private errorLog(message: string): void {
    console.error(`[LuffyDBClient ERROR] ${message}`);
  }

  // Centralized fetch with logging and error handling.
  private async request(url: string, options: RequestInit): Promise<any> {
    let dataIfAny = "";

    try {
      options.headers = options.headers || {};
      (options.headers as Record<string, string>)['luffyclientv'] = "2025.1";

      this.log(`Request: ${options.method || 'GET'} ${url}`);
      const response = await fetch(url, options);
      if (!response.ok) {
        const errorData = await response.json();
        this.errorLog(`HTTP ${response.status} - ${response.statusText}: ${errorData.error || errorData.message || 'Unknowk Error'}`);
        return null;
      }
      const data = await response.json();
      this.log(`Response: ${JSON.stringify(data)}`);
      return data;
    } catch (error: any) {
      this.errorLog(`Fetch error: ${error.message}`);
      return null;
    }
  }

  async defineTable(tableName: string, columns: string[]): Promise<void> {
    const url = `${this.baseUrl}/table/${tableName}/columns`;
    const data = await this.request(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'authorization': this.authKey },
      body: JSON.stringify({ columns }),
    });
    if (data === null) {
      this.errorLog(`Failed to define table "${tableName}"`);
    }
  }

  async listTables(): Promise<void> {
    const url = `${this.baseUrl}/table/list`;
    const data = await this.request(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'authorization': this.authKey },
    });
    if (data === null) {
      this.errorLog(`Failed to list tables`);
      return;
    }

    return data.tables;
  }

  async cleanBackup(tableName: string): Promise<void> {
    const url = `${this.baseUrl}/table/${tableName}/clear-backup`;
    const data = await this.request(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'authorization': this.authKey },
    });
    if (data === null) {
      this.errorLog(`Failed to clean table data`);
    }
  }

  async insert(tableName: string, row: Record<string, any>): Promise<string> {
    const url = `${this.baseUrl}/table/${tableName}/rows`;
    const data = await this.request(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'authorization': this.authKey },
      body: JSON.stringify(row),
    });
    if (data === null) {
      this.errorLog(`Failed to insert row into table "${tableName}"`);
      return '';
    }
    return data.rowId;
  }

  async update(tableName: string, rowId: string, updates: Record<string, any>): Promise<void> {
    const url = `${this.baseUrl}/table/${tableName}/rows/${rowId}`;
    const data = await this.request(url, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json', 'authorization': this.authKey },
      body: JSON.stringify(updates),
    });
    if (data === null) {
      this.errorLog(`Failed to update row "${rowId}" in table "${tableName}"`);
    }
  }

  async delete(tableName: string, rowId: string): Promise<void> {
    const url = `${this.baseUrl}/table/${tableName}/rows/${rowId}`;
    const data = await this.request(url, {
      method: 'DELETE',
      headers: {
        'authorization': this.authKey
      }
    });
    if (data === null) {
      this.errorLog(`Failed to delete row "${rowId}" in table "${tableName}"`);
    }
  }

  async query(
    tableName: string,
    query: Record<string, any> = {},
    limit?: number,
    like?: Record<string, string>
  ): Promise<any[]> {
    const urlObj = new URL(`${this.baseUrl}/table/${tableName}/rows`);
    Object.entries(query).forEach(([key, value]) =>
      urlObj.searchParams.append(key, String(value))
    );
    if (limit) {
      urlObj.searchParams.append('limit', limit.toString());
    }
    if (like) {
      urlObj.searchParams.append('like', JSON.stringify(like));
    }
    const data = await this.request(urlObj.toString(), {
      method: 'GET', headers: {
        'authorization': this.authKey
      }
    });
    if (data === null) {
      this.errorLog(`Failed to query table "${tableName}"`);
      return [];
    }
    return data;
  }
}
