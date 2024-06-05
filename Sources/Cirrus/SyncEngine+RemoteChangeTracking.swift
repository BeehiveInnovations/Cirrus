@_implementationOnly import CKRecordCoder
import CloudKit
import CloudKitCodable
import Foundation
import os.log

extension SyncEngine {
  
  // MARK: - Public
  
  /// Resets the delta change token to fetch everything during next sync.
  /// Use when a full pull is requred
  public func resetChangeToken() {
    self.workQueue.async { [weak self] in
      guard let self else { return }
      
      self.privateChangeToken = nil
    }
  }

  func fetchRemoteChanges() {
    logHandler("\(#function)", .debug)

    var changedRecords: [CKRecord] = []
    var deletedRecordIDs: [CKRecord.ID] = []

    let operation = CKFetchRecordZoneChangesOperation()

    let token: CKServerChangeToken? = privateChangeToken

    let config = CKFetchRecordZoneChangesOperation.ZoneConfiguration(
      previousServerChangeToken: token,
      resultsLimit: nil,
      desiredKeys: nil
    )

    operation.configurationsByRecordZoneID = [zoneIdentifier: config]

    operation.recordZoneIDs = [zoneIdentifier]
    operation.fetchAllChanges = true

    // Take individually changed record
    operation.recordChangedBlock = { [weak self] record in
      guard let self else { return }
      
      self.workQueue.async {
        changedRecords.append(record)
      }
    }

    // Take individually deleted record
    operation.recordWithIDWasDeletedBlock = { [weak self] recordID, recordType in
      guard let self else { return }
      
      self.workQueue.async { [weak self] in
        guard let self else { return }
        
        guard self.recordType == recordType else { return }
        
        deletedRecordIDs.append(recordID)
      }
    }
    
    // The closure to execute when the change token updates. This can be an intermediate
    // change token in during the many fetches it may need to perform. We must NOT
    // save this token until the observer has had a chance to process the changes.
    //
    // When we're here, we emit changes along with the intermediate change token and wait for more changes to come in
    operation.recordZoneChangeTokensUpdatedBlock = { [weak self] _, newChangeToken, _ in
      guard let self else { return }
      
      guard let newChangeToken else { return }
      
      // The fetch may have failed halfway through, so we need to save the change token,
      // emit the current records, and then clear the arrays so we can re-request for the
      // rest of the data.
      self.workQueue.async { [weak self] in
        guard let self else { return }
        
        self.emitServerChanges(with: changedRecords, deletedRecordIDs: deletedRecordIDs, andChangeToken: newChangeToken)
        
        changedRecords.removeAll()
        deletedRecordIDs.removeAll()
      }
    }
    
    // Called after the record zone fetch completes
    operation.recordZoneFetchCompletionBlock = { [weak self] _, newChangeToken, _, _, error in
      guard let self else { return }
      
      if let error = error as? CKError {
        self.logHandler("Failed to fetch record zone changes: \(String(describing: error))", .error)
        
        if error.code == .changeTokenExpired {
          self.logHandler("Change token expired, resetting token and trying again", .error)
          
          self.workQueue.async { [weak self] in
            guard let self else { return }
            
            self.resetChangeToken()
            self.fetchRemoteChanges()
          }
        }
        else {
          error.retryCloudKitOperationIfPossible(self.logHandler, queue: self.workQueue) {
            self.fetchRemoteChanges()
          }
        }
      }
      else {
        self.workQueue.async { [weak self] in
          guard let self else { return }

          if deletedRecordIDs.isEmpty && changedRecords.isEmpty {
            self.logHandler("Fetch completed", .info)
          }
          else {
            self.logHandler("Finalizing fetch...", .info)
          }

          self.emitServerChanges(with: changedRecords,
                                 deletedRecordIDs: deletedRecordIDs,
                                 andChangeToken: newChangeToken)
          
          changedRecords.removeAll()
          deletedRecordIDs.removeAll()
        }
      }
    }


    // Called after the entire fetch operation completes for all zones
    operation.fetchRecordZoneChangesCompletionBlock = { [weak self] error in
      guard let self else { return }

      if let error {
        self.logHandler("Failed to fetch record zone changes: \(String(describing: error))", .error)

        error.retryCloudKitOperationIfPossible(self.logHandler, queue: self.workQueue) {
          self.fetchRemoteChanges()
        }
      } 
      else {
        self.logHandler("Finished fetching record zone changes", .info)
      }
    }

    operation.qualityOfService = .userInitiated
    operation.database = privateDatabase

    cloudOperationQueue.addOperation(operation)
    
    // we want to wait for the fetch to complete before pushing changes
    cloudOperationQueue.waitUntilAllOperationsAreFinished()
  }

  // MARK: - Private

  internal var privateChangeToken: CKServerChangeToken? {
    get {
      guard let data = defaults.data(forKey: privateChangeTokenKey) else { return nil }
      guard !data.isEmpty else { return nil }

      do {
        let token = try NSKeyedUnarchiver.unarchivedObject(
          ofClass: CKServerChangeToken.self, from: data)

        return token
      } catch {
        logHandler(
          "Failed to decode CKServerChangeToken from defaults key privateChangeToken", .error)
        return nil
      }
    }
    set {
      guard let newValue = newValue else {
        defaults.setValue(Data(), forKey: privateChangeTokenKey)
        return
      }

      do {
        let data = try NSKeyedArchiver.archivedData(
          withRootObject: newValue, requiringSecureCoding: true)

        defaults.set(data, forKey: privateChangeTokenKey)
      } catch {
        logHandler(
          "Failed to encode private change token: \(String(describing: error))", .error)
      }
    }
  }

  private func emitServerChanges(with changedRecords: [CKRecord], 
                                 deletedRecordIDs: [CKRecord.ID],
                                 andChangeToken changeToken: CKServerChangeToken?) {
    guard !changedRecords.isEmpty || !deletedRecordIDs.isEmpty else {
      logHandler("Finished record zone changes fetch with no changes", .info)
      return
    }

    logHandler("Will emit \(changedRecords.count) changed record(s) and \(deletedRecordIDs.count) deleted record(s)", .info)

    let models: Set<Model> = Set(
      changedRecords.compactMap { record in
        do {
          let decoder = CKRecordDecoder()
          return try decoder.decode(Model.self, from: record)
        } catch {
          logHandler(
            "Error decoding item from record: \(String(describing: error))", .error)
          return nil
        }
      })

    let deletedIdentifiers = Set(deletedRecordIDs.map(\.recordName))

    modelsChangedSubject.send(.init(updates: .updatesPulled(models),
                                    deletes: .deletesPulled(deletedIdentifiers),
                                    changeToken: changeToken))
  }
}
