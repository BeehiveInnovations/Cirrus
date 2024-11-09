import CKRecordCoder
import CloudKit
import CloudKitCodable
import Foundation
import os.log

extension SyncEngine {
  
  // MARK: - Public
  
  /// Fetch remote changes
  /// - Parameter onCompletion: pass a completion block that gets called only after changes have beel pulled, emitted and consumed
  func fetchRemoteChanges(onCompletion: @escaping ((Result<SyncEngine<Model>.ModelChanges, Error>) -> Void)) {
    logHandler("\(#function)", .debug)

    // track if our fetch failed and we're retrying
    var operationRetrying = false
    var changedRecords: [CKRecord] = []
    var deletedRecordIDs = Set<CKRecord.ID>()
    var finalChangeToken: CKServerChangeToken? = privateChangeToken
    
    // Completion handler invocation guard
    var completionCalled = false
    let callCompletion: (Result<SyncEngine<Model>.ModelChanges, Error>) -> Void = { result in
      if !completionCalled {
        completionCalled = true
        onCompletion(result)
      }
    }
    
    let operation = CKFetchRecordZoneChangesOperation()
    
    self.currentFetchOperation = operation // This will cancel any existing operatio/

    let config = CKFetchRecordZoneChangesOperation.ZoneConfiguration(previousServerChangeToken: finalChangeToken)

    // Force CloudKit to ignore client caching
    operation.configuration.isLongLived = false
    operation.configuration.timeoutIntervalForResource = 30
    operation.configuration.qualityOfService = .userInitiated
    operation.configurationsByRecordZoneID = [zoneIdentifier: config]

    operation.recordZoneIDs = [zoneIdentifier]
    operation.fetchAllChanges = true
    
    // Handle zone fetch failure with retry
    let handleZoneFetchFailure: (Error) -> Void = { [weak self] error in
      guard let self = self else { return }
      
      self.logHandler("Zone fetch failed: \(error.localizedDescription)", .error)
      
      if let ckError = error as? CKError {
        switch ckError.code {
          case .changeTokenExpired:
            operationRetrying = true
            
            self.workQueue.async { [weak self] in
              guard let self = self else { return }
              self.logHandler("Change token expired, resetting change token and trying again", .error)
              self.privateChangeToken = nil
              
              DispatchQueue.main.async {
                self.fetchRemoteChanges(onCompletion: callCompletion)
              }
            }
          default:
            // Only retry if it's a retryable error
            let result = ckError.retryCloudKitOperationIfPossible(self.logHandler, queue: self.workQueue) {
              self.fetchRemoteChanges(onCompletion: callCompletion)
            }
            
            if !result {
              logHandler("Fetch is not possible: \( String(describing: error))", .error)
              
              self.workQueue.async {
                callCompletion(.failure(error))
              }
            }
            else {
              operationRetrying = true
            }
        }
      }
      else {
        self.workQueue.async {
          callCompletion(.failure(error))
        }
      }
    }
  
    // Take individually deleted record
    operation.recordWithIDWasDeletedBlock = { [weak self] recordID, recordType in
      guard let self else { return }
      
      self.workQueue.async { [weak self] in
        guard let self else { return }
        
        guard self.recordType == recordType else {
          self.logHandler("... fetched unknown deleted item, ignored: \(recordType), \(recordID)", .info)
          
          return
        }
        
        //self.logHandler("... fetched deleted item", .debug)
        
        self.lastChangeDate = Date()
        
        deletedRecordIDs.insert(recordID)
      }
    }
    
    // Cache intermediate change tokens
    operation.recordZoneChangeTokensUpdatedBlock = { recordZoneID, token, _ in
      if token != finalChangeToken {
#if DEBUG
        self.logHandler("Change token got updated during fetch: \(token.debugDescription)", .default)
#else
        self.logHandler("Change token got updated during fetch", .default)
#endif
      }
      
      finalChangeToken = token
    }

    // Take individually changed record
    
    if #available(macOS 12.0, iOS 15.0, tvOS 15.0, watchOS 8.0, *) {
      operation.recordWasChangedBlock = { [weak self] recordID, result in
        guard let self else { return }
        
        switch result {
          case .success(let record):
            self.workQueue.async {
              self.lastChangeDate = Date()
              
              changedRecords.append(record)
            }
            
          case .failure(let error):
            // Handle per-record failures
            self.workQueue.async {
              self.logHandler("Failed to fetch record \(recordID.recordName): \(error)", .error)
              
              // Optionally handle specific error cases
              if let ckError = error as? CKError {
                switch ckError.code {
                  case .serverRecordChanged:
                    // Record was changed while we were fetching
                    break
                  case .unknownItem:
                    // Record no longer exists
                    self.logHandler("Record no longer exists: \(recordID.recordName)", .info)
                    
                    self.lastChangeDate = Date()
                    
                    deletedRecordIDs.insert(recordID)
                    
                  default:
                    break
                }
              }
            }
        }
      }
      
      // Called after the record zone fetch completes
      operation.recordZoneFetchResultBlock = { [weak self] zoneID, result in
        guard let self else { return }
        
        switch result {
          case .success(let fetchResult):
            self.workQueue.async { [weak self] in
              guard let self else { return }
              
              // Update token if provided
              let newToken = fetchResult.serverChangeToken
              
              if newToken != finalChangeToken {
#if DEBUG
                self.logHandler("Zone fetch success - new change token received: \(newToken)", .debug)
#endif
                finalChangeToken = newToken
              }
              else {
#if DEBUG
                self.logHandler("Zone fetch success - change token not changed", .debug)
#endif
              }
              
              // Log progress
              let changeCount = changedRecords.count
              let deleteCount = deletedRecordIDs.count
              
              if changeCount > 0 || deleteCount > 0 {
                self.logHandler("""
                    Zone fetch progress:
                    - Zone: \(zoneID.zoneName)
                    - Changes: \(changeCount)
                    - Deletes: \(deleteCount)
                    - More Coming: \(fetchResult.moreComing ? "Yes" : "No")
                    """, .debug)
              }
              else {
                self.logHandler("Zone fetch completed - no changes", .debug)
              }
            }
            
          case .failure(let error):
            self.logHandler("recordZoneFetchResultBlock failed: \(error.localizedDescription)", .debug)
            
            handleZoneFetchFailure(error)
        }
      }
      
      // Called after the entire fetch operation completes for all zones
      //
      // Important: This block is called immediately after recordZoneFetchResultBlock,
      // so even if `recordZoneFetchResultBlock` got called with a failure such as for an expired token,
      // the following block will be called with a `success` and NOT a failure!
      operation.fetchRecordZoneChangesResultBlock = { [weak self] result in
        guard let self else { return }
        
        guard operationRetrying == false else {
          self.logHandler("Finished fetching record zone changes, nothing to do as we're retrying", .debug)
          return
        }
        
        switch result {
          case .failure(let error):
            self.logHandler("Failed to fetch record zone changes: \(String(describing: error))", .error)
            
            let result = error.retryCloudKitOperationIfPossible(self.logHandler, queue: self.workQueue) {
              self.fetchRemoteChanges(onCompletion: callCompletion)
            }
            
            if !result {
              logHandler("Fetch operation is not possible: \( String(describing: error))", .error)
              
              self.workQueue.async {
                callCompletion(.failure(error))
              }
            }
            
          case .success:
            self.logHandler("Finished fetching record zone changes", .info)
            
            self.workQueue.async { [weak self] in
              guard let self else { return }
              
              if deletedRecordIDs.isEmpty && changedRecords.isEmpty {
                self.logHandler("Fetch operation completed [\(changedRecords.count) changes, \(deletedRecordIDs.count) deletes]", .info)
              }
              else {
                self.logHandler("Finalizing fetch operation [\(changedRecords.count) changes, \(deletedRecordIDs.count) deletes]", .info)
              }
              
              callCompletion(.success(makeModelChanges(with: changedRecords,
                                                       deletedRecordIDs: Array(deletedRecordIDs),
                                                       andChangeToken: finalChangeToken)))
            }
        }
      }
    }
    else {
      operation.recordChangedBlock = { [weak self] record in
        guard let self else { return }
        
        self.workQueue.async {
          self.lastChangeDate = Date()
          
          changedRecords.append(record)
          
          //        self.logHandler("... fetched changed item", .debug)
        }
      }
     
      // Called after the record zone fetch completes
      operation.recordZoneFetchCompletionBlock = { [weak self] _, newChangeToken, _, isFinalChange, error in
        guard let self else { return }
        
        if let error = error as? CKError {
          self.logHandler("recordZoneFetchCompletionBlock failure: \(error.localizedDescription)", .debug)
          
          handleZoneFetchFailure(error)
        }
        else {
          self.workQueue.async { [weak self] in
            guard let self else { return }
            
            if deletedRecordIDs.isEmpty && changedRecords.isEmpty {
              self.logHandler("Zone fetch completed [\(changedRecords.count) changes, \(deletedRecordIDs.count) deletes], final? \(isFinalChange)", .info)
            }
            else {
              self.logHandler("Zone fetch finished [\(changedRecords.count) changes, \(deletedRecordIDs.count) deletes], final? \(isFinalChange)", .info)
            }
            
            if newChangeToken != finalChangeToken {
#if DEBUG
              self.logHandler("Change token got updated after zone fetch completion: \(newChangeToken.debugDescription)", .default)
#else
              self.logHandler("Change token got updated after zone fetch completion", .default)
#endif
            }
            else {
#if DEBUG
              self.logHandler("Change token is same after zone fetch completion: \(newChangeToken.debugDescription)", .default)
#endif
            }
            
            // Track but don't save till the entire fetch operation is finished
            finalChangeToken = newChangeToken
          }
        }
      }
      
      // Called after the entire fetch operation completes for all zones
      operation.fetchRecordZoneChangesCompletionBlock = { [weak self] error in
        guard let self else { return }
        
        guard operationRetrying == false else {
          self.logHandler("Finished fetching record zone changes, nothing to do as we're retrying", .debug)
          return
        }
        
        if let error {
          self.logHandler("Failed to fetch record zone changes: \(String(describing: error))", .error)
          
          let result = error.retryCloudKitOperationIfPossible(self.logHandler, queue: self.workQueue) {
            self.fetchRemoteChanges(onCompletion: callCompletion)
          }
          
          if !result {
            logHandler("Fetch operation is not possible: \( String(describing: error))", .error)
            
            self.workQueue.async {
              callCompletion(.failure(error))
            }
          }
        }
        else {
          self.logHandler("Finished fetching record zone changes", .info)
          
          self.workQueue.async { [weak self] in
            guard let self else { return }
            
            if deletedRecordIDs.isEmpty && changedRecords.isEmpty {
              self.logHandler("Fetch operation completed [\(changedRecords.count) changes, \(deletedRecordIDs.count) deletes]", .info)
            }
            else {
              self.logHandler("Finalizing fetch operation [\(changedRecords.count) changes, \(deletedRecordIDs.count) deletes]", .info)
            }
            
            callCompletion(.success(makeModelChanges(with: changedRecords,
                                                     deletedRecordIDs: Array(deletedRecordIDs),
                                                     andChangeToken: finalChangeToken)))
          }
        }
      }
    }

    operation.database = privateDatabase

    cloudOperationQueue.addOperation(operation)
  }

  // MARK: - Private

  internal var privateChangeToken: CKServerChangeToken? {
    get {
      guard let data = defaults.data(forKey: privateChangeTokenKey) else {
#if DEBUG
        logHandler("Previous change token not found for: \(privateChangeTokenKey)", .debug)
#else
        logHandler("Previous change token not found", .default)
#endif
        
        return nil
      }
      guard !data.isEmpty else {
#if DEBUG
        logHandler("Previous change token is empty for: \(privateChangeTokenKey)", .debug)
#else
        logHandler("Previous change token is empty", .default)
#endif
        return nil
      }

      do {
        let token = try NSKeyedUnarchiver.unarchivedObject(ofClass: CKServerChangeToken.self, from: data)
                
        logHandler("Previous change token: \(token.debugDescription), for key: \(privateChangeTokenKey)", .debug)

        return token
      } catch {
        logHandler(
          "Failed to decode CKServerChangeToken from defaults key privateChangeToken", .error)
        return nil
      }
    }
    set {
      if newValue != privateChangeToken {
        self.changeTokenSavedDate = Date()
      }

      guard let newValue else {
        defaults.set(Data(), forKey: privateChangeTokenKey)
        
#if DEBUG
        logHandler("Change token reset using key: \(privateChangeTokenKey)", .debug)
#else
        logHandler("Change token reset", .default)
#endif
        return
      }

      do {
        if newValue != privateChangeToken {
          let data = try NSKeyedArchiver.archivedData(withRootObject: newValue, requiringSecureCoding: true)
          
          defaults.set(data, forKey: privateChangeTokenKey)
          
#if DEBUG
          logHandler("Change token saved: \(newValue.debugDescription), key: \(privateChangeTokenKey)", .debug)
#else
          logHandler("Change token updated", .default)
#endif
          defaults.synchronize()
        }
      } catch {
        logHandler(
          "Failed to encode private change token: \(String(describing: error)) for key: \(privateChangeTokenKey)", .error)
      }
    }
  }
    
  /// The time the change token was last saved
  public var changeTokenSavedDate: Date? {
    get {
      guard let date = defaults.value(forKey: privateChangeTokenSavedKey) as? Date else {
        return nil
      }
      return date
    }
    set {
      guard let newValue else {
        defaults.removeObject(forKey: privateChangeTokenSavedKey)
        return
      }
      
      defaults.set(newValue, forKey: privateChangeTokenSavedKey)
    }
  }
  
  /// The last time any update was made on this zone (push / pull)
  public var lastChangeDate: Date? {
    get {
      guard let date = defaults.value(forKey: privateLastChangedateKey) as? Date else {
        return nil
      }
      return date
    }
    set {
      guard let newValue else {
        defaults.removeObject(forKey: privateLastChangedateKey)
        return
      }
      
      defaults.set(newValue, forKey: privateLastChangedateKey)
    }
  }
  
  // MARK: - Helper
  
  /// Wrap changes that need to be returned to the caller
  /// - Parameters:
  ///   - changedRecords: changed records
  ///   - deletedRecordIDs: deleted record IDs
  ///   - changeToken: the new change token for these set of changes. Passed back to the consumer for it to update the SyncEngine after processing changes.
  private func makeModelChanges(with changedRecords: [CKRecord],
                                deletedRecordIDs: [CKRecord.ID],
                                andChangeToken changeToken: CKServerChangeToken? = nil) -> ModelChanges {
    
    if !changedRecords.isEmpty || !deletedRecordIDs.isEmpty {
      logHandler("Fetched \(changedRecords.count) changed record(s) and \(deletedRecordIDs.count) deleted record(s)", .info)
    }
    
    let models: Set<Model> = Set(
      changedRecords.compactMap { record in
        do {
          let decoder = CKRecordDecoder()
          return try decoder.decode(Model.self, from: record)
        } catch {
          logHandler("Error decoding item from record: \(String(describing: error))", .error)
          
          return nil
        }
      })
    
    let deletedIdentifiers = Set(deletedRecordIDs.map(\.recordName))
    
    return .init(updates: .updatesPulled(models),
                 deletes: .deletesPulled(deletedIdentifiers),
                 changeToken: changeToken)
  }
  
  /// Clear out a previously running operation
  internal func clearOperationBlocks(_ operation: CKFetchRecordZoneChangesOperation) {
    logHandler("Clearing previous operation", .debug)
    
    if #available(macOS 12.0, iOS 15.0, tvOS 15.0, watchOS 8.0, *) {
      operation.recordWasChangedBlock = nil
      operation.recordZoneFetchResultBlock = nil
      operation.fetchRecordZoneChangesResultBlock = nil
    } else {
      operation.recordChangedBlock = nil
      operation.recordZoneFetchCompletionBlock = nil
      operation.fetchRecordZoneChangesCompletionBlock = nil
    }
    operation.recordWithIDWasDeletedBlock = nil
    operation.recordZoneChangeTokensUpdatedBlock = nil
  }
}
