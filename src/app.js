const environment = '';

const {SecretManagerServiceClient} = require('@google-cloud/secret-manager');
const {Firestore} = require('@google-cloud/firestore');

const express = require("express");
const path = require('path');

const secretManagerClient = new SecretManagerServiceClient();

const firestore = new Firestore({
    projectId: 'ssfs-202408',
    databaseId: 'ssfs',
    ignoreUndefinedProperties: true
});
const bodyParser = require('body-parser');
const fetch = require('node-fetch');
const app = express();
const fs = require('fs');
const MAX_REQUESTS_PER_MINUTE = 20;
const REQUEST_INTERVAL = 60000 / MAX_REQUESTS_PER_MINUTE;

app.use(bodyParser.json());

const { Logging } = require('@google-cloud/logging');
const JSONStream = require('JSONStream');
const { Storage } = require('@google-cloud/storage');
const storage = new Storage();

class RateLimiter {
    constructor(maxRequests, interval) {
        this.maxRequests = maxRequests;
        this.interval = interval;
        this.queue = [];
        this.currentRequests = 0;
        this.isProcessing = false;
    }

    async addRequest(requestFn) {
        return new Promise((resolve, reject) => {
            this.queue.push({ requestFn, resolve, reject });
            this.processQueue();
        });
    }

    async processQueue() {
        if (this.isProcessing) return;
        this.isProcessing = true;

        while (this.queue.length > 0 && this.currentRequests < this.maxRequests) {
            const { requestFn, resolve, reject } = this.queue.shift();
            this.currentRequests++;

            try {
                const response = await requestFn();
                resolve(response);
            } catch (error) {
                reject(error);
                console.error('Error in request:', error);
            } finally {
                this.currentRequests--;
                if (this.queue.length > 0) {
                    await new Promise(r => setTimeout(r, this.interval));
                }
            }
        }

        this.isProcessing = false;
    }
}

const rateLimiter = new RateLimiter(MAX_REQUESTS_PER_MINUTE, REQUEST_INTERVAL);

async function getSecretFromGCP(secretName) {
    try {

        let projectId = process.env.GOOGLE_CLOUD_PROJECT;
        if (!projectId) {
            projectId = 'ssfs-202408';
        }
        const name = `projects/${projectId}/secrets/${secretName}/versions/latest`;
        
        const [version] = await secretManagerClient.accessSecretVersion({
            name: name,
        });
        
        return version.payload.data.toString();
    } catch (error) {
        console.error(`Error fetching secret ${secretName}:`, error);
        throw error;
    }
}

async function getAssistantIdFromFirestore(subscriptionId) {
    // Check if subscription ID is provided
    if (!subscriptionId) {
        console.error('subscriptionId is required for fetching AssistantID');
        throw new Error('subscriptionId is required');
    }

    try {
        console.log(`Fetching AssistantID for subscription: ${subscriptionId}`);
        
        const quotaDoc = firestore.collection('subscriptions').doc(subscriptionId);
        const doc = await quotaDoc.get();

        if (!doc.exists) {
            console.error(`Subscription ID not found: ${subscriptionId}`);
            throw new Error('Subscription ID not found');
        }

        // Get data from document
        const data = doc.data();
        if (data.AssistantID !== undefined) {
            console.log(`Found AssistantID: ${data.AssistantID}`);
            return data.AssistantID;
        } else {
            console.error(`AssistantID not found for subscription: ${subscriptionId}`);
            throw new Error('AssistantID not found in Firestore');
        }
    } catch (error) {
        console.error(`Error fetching AssistantID: ${error.message}`);
        throw error;
    }
}

async function getJsonSchema() {
    try {
        const jsonSchema = fs.readFileSync('./src/service-definition.json', 'utf8');
        return JSON.parse(jsonSchema);
    } catch (err) {
        console.error('Error reading JSON schema:', err);
        return null;
    }
}

async function readJsonFromStorage(bucketName, fileName) {
    const [content] = await storage.bucket(bucketName).file(fileName).download();
    return JSON.parse(content.toString('utf8'));
}

async function logEntry(projectId = 'Ssfs-202408', logName = 'ssfs-202408', req, isText = false) {
    let text = req;
    if (!isText) {
        text = JSON.stringify(req.body);
    }
    console.log(`Logged: ${text}`);
}

app.use('/service-definition.json', express.static(path.join(__dirname, 'service-definition.json')));

app.get("/getServiceDefinition", async (req, res) => {
    const response = await getJsonSchema();
    res.status(200).json(response);
});

app.get("/status", (req, res) => {
    res.json({ status: "ok" });
});

app.get("/brandIcon", (req, res) => {
    try {
        const png = fs.readFileSync('./src/logo.png');
        res.set('Content-Type', 'image/png');
        res.send(png);
    } catch (err) {
        console.error('Error reading icon file:', err);
    }
});

app.get("/serviceIcon", (req, res) => {
    try {
        const png = fs.readFileSync('./src/logo.png');
        res.set('Content-Type', 'image/png');
        res.send(png);
    } catch (err) {
        console.error('Error reading icon file:', err);
    }
});

async function getPromptValue(prompt) {
    return prompt;
}

async function processLead(obj, token, apiCallBackKey, campaignId, callbackUrl, context, resolve) {
    const lead = obj.objectContext;
    const flowStepContext = obj.flowStepContext;
    const prompt = `${flowStepContext.ChatGPTPrompt || ''}\n${lead.Prompt || ''}`.trim();
    const systemPrompt = context.admin.ChatGPTSystemPrompt; // Original ref (retained for backward compatibility, can remove)
    if (!prompt) {
        resolve({
            leadData: { id: lead.id, AI_Description: "Prompt missing", AI_Score: "N/A", AI_Category: "N/A" },
            activityData: { success: false }
        });
        return;
    }

    console.log('Prompt being sent to Assistant:', prompt);
    const response = await queryChatGPT(prompt, context);
    console.log('Raw response from Assistant:', response);

    try {
        const cleaned = JSON.parse(response);
        const aiDescription = cleaned["AI Description"] || "No description";
        const aiScore = cleaned["AI Score"] || "No score";
        const aiCategory = cleaned["AI Category"] || "Uncategorized";

        resolve({
            leadData: { id: lead.id, AI_Description: aiDescription, AI_Score: aiScore, AI_Category: aiCategory },
            activityData: { success: true, AI_Description: aiDescription, AI_Score: aiScore, AI_Category: aiCategory }
        });
    } catch (err) {
        console.error("Failed to parse AI response:", err);
        resolve({
            leadData: { id: lead.id, AI_Description: "Invalid JSON", AI_Score: "N/A", AI_Category: "N/A" },
            activityData: { success: false }
        });
    }
}

async function queryChatGPT(prompt, context) {
    // Get API key from GCP Secret Manager
    const munchkinId = context.subscription.munchkinId;
    const apiKey = await getSecretFromGCP(`ssfs-ethan-oct16-openai-apikey-${munchkinId}`);
    
    const assistantId = await getAssistantIdFromFirestore(munchkinId);

    const headers = {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`,
        "OpenAI-Beta": "assistants=v2"
    };

    const threadRes = await fetch("https://api.openai.com/v1/threads", {
        method: "POST",
        headers
    });
    const thread = await threadRes.json();

    await fetch(`https://api.openai.com/v1/threads/${thread.id}/messages`, {
        method: "POST",
        headers,
        body: JSON.stringify({ role: "user", content: prompt })
    });

    const runRes = await fetch(`https://api.openai.com/v1/threads/${thread.id}/runs`, {
        method: "POST",
        headers,
        body: JSON.stringify({ assistant_id: assistantId })
    });
    const run = await runRes.json();

    let runStatus;
    let retryCount = 0;
    const maxRetries = 10;
    do {
        if (retryCount === maxRetries - 1) {
            return JSON.stringify({ "AI Description": "Max retries reached", "AI Score": "N/A", "AI Category": "N/A" });
        }
        const statusRes = await fetch(`https://api.openai.com/v1/threads/${thread.id}/runs/${run.id}`, {
            method: "GET",
            headers
        });
        runStatus = await statusRes.json();
        if (runStatus.status === "completed") break;
        if (["failed", "cancelled"].includes(runStatus.status)) {
            return JSON.stringify({ "AI Description": `Run failed: ${runStatus.status}`, "AI Score": "N/A", "AI Category": "N/A" });
        }
        await new Promise(r => setTimeout(r, 5000));
    } while (++retryCount < maxRetries);

    const messagesRes = await fetch(`https://api.openai.com/v1/threads/${thread.id}/messages`, {
        method: "GET",
        headers
    });
    const messages = await messagesRes.json();
    return messages.data?.[0]?.content?.[0]?.text?.value || JSON.stringify({ "AI Description": "No message content", "AI Score": "N/A", "AI Category": "N/A" });
}

async function processData(objectData, token, apiCallBackKey, campaignId, callbackUrl, context) {
    const objectDataReponse = [];

    for (const key in objectData) {
        const obj = objectData[key];
        const processedLead = rateLimiter.addRequest(() => new Promise(resolve => {
            processLead(obj, token, apiCallBackKey, campaignId, callbackUrl, context, resolve);
        }));
        objectDataReponse.push(processedLead);
    }

    try {
        return await Promise.all(objectDataReponse);
    } catch (error) {
        console.error('Error processing requests:', error);
        return [];
    }
}

app.post("/submitAsyncActionService", async (req, res) => {
    const { bucketName, filename } = req.body;
    const requestJSONParsed = await readJsonFromStorage(bucketName, filename);
    const { token, apiCallBackKey, campaignId, callbackUrl, context, objectData } = requestJSONParsed;

    logEntry('Ssfs-202408', 'ssfs-202408', 'Received /submitAsyncAction', true);
    logEntry('Ssfs-202408', 'ssfs-202408', req);

    const objectDataReponse = await processData(objectData, token, apiCallBackKey, campaignId, callbackUrl, context);

    const callbackBody = {
        munchkinId: context.subscription.munchkinId,
        token,
        objectData: objectDataReponse
    };

    const callbackPayload = {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${token}`,
            "x-api-key": apiCallBackKey
        },
        body: JSON.stringify(callbackBody)
    };

    const callBackResponse = await fetch(callbackUrl, callbackPayload);
    await callBackResponse.json();

    logEntry('Ssfs-202408', 'ssfs-202408', 'callBackResponse is sent to Marketo', true);
    res.status(201).json({ message: "Request accepted for processing" });
});

let port = 8080;

if (environment === 'dev') {
    port = 8081;
    app.listen(port, () => {
        console.log(`Server started on port ${port}`);
    });
    module.exports.app = app;
} else {
    app.listen(port, () => {
        console.log(`Server started on port ${port}`);
    });
}
