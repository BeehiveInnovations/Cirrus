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

    var changedRecords: [CKRecord] = []
    var deletedRecordIDs: [CKRecord.ID] = []
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

    let config = CKFetchRecordZoneChangesOperation.ZoneConfiguration(previousServerChangeToken: finalChangeToken)

    operation.configurationsByRecordZoneID = [zoneIdentifier: config]

    operation.recordZoneIDs = [zoneIdentifier]
    operation.fetchAllChanges = true

    // Take individually changed record
    operation.recordChangedBlock = { [weak self] record in
      guard let self else { return }
      
      self.workQueue.async {
        changedRecords.append(record)

//        self.logHandler("... fetched changed item", .debug)
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

        deletedRecordIDs.append(recordID)
      }
    }
    
    // Cache intermediate change tokens
    operation.recordZoneChangeTokensUpdatedBlock = { recordZoneID, token, _ in
      finalChangeToken = token
    }
    
    // Called after the record zone fetch completes
    operation.recordZoneFetchCompletionBlock = { [weak self] _, newChangeToken, _, isFinalChange, error in
      guard let self else { return }
      
      if let error = error as? CKError {
        self.logHandler("Failed to fetch record zone changes (final? \(isFinalChange ? "yes" : "no")): \(String(describing: error))", .error)
        
        if error.code == .changeTokenExpired {
          self.workQueue.async { [weak self] in
            guard let self else { return }

            self.logHandler("Change token expired, resetting token and trying again", .error)
            
            self.privateChangeToken = nil
            self.fetchRemoteChanges(onCompletion: callCompletion)
          }
        }
        else {
          let result = error.retryCloudKitOperationIfPossible(self.logHandler, queue: self.workQueue) {
            self.fetchRemoteChanges(onCompletion: callCompletion)
          }
          
          if !result {
            logHandler("Fetch is not possible: \( String(describing: error))", .error)
            
            self.workQueue.async {
              callCompletion(.failure(error))
            }
          }
        }
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
          
          finalChangeToken = newChangeToken
        }
      }
    }


    // Called after the entire fetch operation completes for all zones
    operation.fetchRecordZoneChangesCompletionBlock = { [weak self] error in
      guard let self else { return }

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
                                                   deletedRecordIDs: deletedRecordIDs,
                                                   andChangeToken: finalChangeToken)))
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
        let data = try NSKeyedArchiver.archivedData(withRootObject: newValue, requiringSecureCoding: true)

        defaults.set(data, forKey: privateChangeTokenKey)
        
#if DEBUG
        logHandler("Change token updated to: \(newValue.debugDescription), key: \(privateChangeTokenKey)", .debug)
#else
        logHandler("Change token updated", .default)
#endif
        defaults.synchronize()
      } catch {
        logHandler(
          "Failed to encode private change token: \(String(describing: error)) for key: \(privateChangeTokenKey)", .error)
      }
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
}
